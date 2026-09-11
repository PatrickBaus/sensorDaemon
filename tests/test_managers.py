"""Unit tests for the sensor manager implementations."""

# The tests intentionally exercise private helpers and asynchronous manager loops.
# pylint: disable=protected-access,redefined-outer-name,too-many-public-methods

import asyncio
import errno
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock, call
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

import managers
from data_types import DataEvent
from errors import UnknownDriverError


class FakeMqttClient:
    """Minimal asynchronous MQTT client used by consumer tests."""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.publish = AsyncMock()

    async def __aenter__(self):
        """Enter the fake MQTT client context."""
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        """Leave the fake MQTT client context."""
        return None


@pytest.fixture
def broker_params():
    """Return valid MQTT broker parameters."""
    return managers.MQTTParams(hosts="broker.example:1883", username="user", password="secret")


@pytest.fixture
def mqtt_manager(broker_params):
    """Return an MQTT manager with a deterministic node ID."""
    return managers.MqttManager(uuid4(), broker_params, number_of_workers=2)


class TestMQTTParams:
    """Tests for MQTT broker parameter validation."""

    def test_single_host_uses_explicit_port(self):
        """Parse a single host and explicit port."""
        params = managers.MQTTParams(hosts="broker.example:1234", username=None, password=None)

        assert params.hosts == [("broker.example", 1234)]

    def test_single_host_uses_default_port(self):
        """Use MQTT's default port when no port is supplied."""
        params = managers.MQTTParams(hosts="broker.example", username=None, password=None)

        assert params.hosts == [("broker.example", 1883)]

    def test_zero_port_uses_default_port(self):
        """Treat port zero as a request for the default port."""
        params = managers.MQTTParams(hosts="broker.example:0", username=None, password=None)

        assert params.hosts == [("broker.example", 1883)]

    def test_multiple_hosts_are_split_and_stripped(self):
        """Parse and trim a comma-separated host list."""
        params = managers.MQTTParams(hosts=" broker1.example:1883,broker2.example:1884 ", username=None, password=None)

        assert params.hosts == [("broker1.example", 1883), ("broker2.example", 1884)]

    @pytest.mark.parametrize("host", ["", "-invalid.example", "host:65536", "host:abc", "host..example"])
    def test_invalid_host_is_rejected(self, host):
        """Reject malformed hostnames and ports."""
        with pytest.raises((ValidationError, ValueError)):
            managers.MQTTParams(hosts=host, username=None, password=None)

    def test_maximum_port_is_accepted(self):
        """Accept port 65535."""
        params = managers.MQTTParams(hosts="broker.example:65535", username=None, password=None)

        assert params.hosts == [("broker.example", 65535)]

    def test_credentials_can_be_none(self):
        """Allow unauthenticated broker configuration."""
        params = managers.MQTTParams(hosts="broker.example", username=None, password=None)

        assert params.username is None
        assert params.password is None


class TestMqttManager:
    """Tests for MQTT manager behavior."""

    def test_calculate_timeout_before_interval_has_elapsed(self, monkeypatch):
        """Return the remaining reconnect delay."""
        loop = Mock()
        loop.time.return_value = 103.0
        monkeypatch.setattr(asyncio, "get_running_loop", Mock(return_value=loop))

        assert managers.MqttManager._calculate_timeout(100.0, 5.0) == 2.0

    def test_calculate_timeout_never_returns_negative(self, monkeypatch):
        """Clamp elapsed reconnect intervals to zero."""
        loop = Mock()
        loop.time.return_value = 110.0
        monkeypatch.setattr(asyncio, "get_running_loop", Mock(return_value=loop))

        assert managers.MqttManager._calculate_timeout(100.0, 5.0) == 0.0

    @pytest.mark.parametrize(
        "error_code,fragment",
        [
            (4, "currently not connected"),
            (7, "connection to MQTT broker"),
            (111, "Connection refused"),
            (113, "unreachable"),
            (128, "unspecified error"),
            (134, "Invalid username or password"),
            (-2, "Failure in name resolution"),
            (-3, "Temporary failure in name resolution"),
            (-5, "Unknown host name"),
            ("timed out", "timed out"),
        ],
    )
    def test_log_mqtt_error_code_logs_known_error(self, mqtt_manager, caplog, error_code, fragment):
        """Log each known MQTT error code as an error."""
        with caplog.at_level("ERROR"):
            mqtt_manager._log_mqtt_error_code("worker", ("broker", 1883), error_code, None)

        assert fragment in caplog.text

    def test_log_mqtt_error_code_suppresses_repeated_error(self, mqtt_manager, caplog):
        """Do not log the same error code twice in succession."""
        with caplog.at_level("ERROR"):
            mqtt_manager._log_mqtt_error_code("worker", ("broker", 1883), 111, 111)

        assert not caplog.records

    def test_log_mqtt_error_code_logs_unknown_error_as_exception(self, mqtt_manager, caplog):
        """Log an unknown MQTT error using the exception logger."""
        with caplog.at_level("ERROR"):
            mqtt_manager._log_mqtt_error_code("worker", ("broker", 1883), 999, None)

        assert "MQTT connection error (code: 999)" in caplog.text

    @pytest.mark.asyncio
    async def test_producer_converts_event_to_queue_payload(self, mqtt_manager, monkeypatch):
        """Convert a DataEvent into the MQTT producer queue format."""
        sender = uuid4()
        event = DataEvent(sender, 7, "temperature", 21.5, "degC")

        async def subscribe(topic):
            """Yield one test event."""
            assert topic == "wamp/publish"
            yield event
            await asyncio.sleep(0)

        monkeypatch.setattr(managers.event_bus, "subscribe", subscribe)
        queue = asyncio.Queue()

        task = asyncio.create_task(mqtt_manager.producer(queue))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        topic, payload = queue.get_nowait()
        assert topic == "temperature"
        assert payload["uuid"] == str(sender)
        assert payload["sid"] == 7
        assert payload["value"] == 21.5
        assert payload["unit"] == "degC"
        assert isinstance(payload["timestamp"], float)

    @pytest.mark.asyncio
    async def test_producer_drops_malformed_event(self, mqtt_manager, monkeypatch, caplog):
        """Drop events that do not expose the DataEvent attributes."""

        async def subscribe(topic):
            """Yield one malformed event."""
            assert topic == "wamp/publish"
            yield object()
            await asyncio.sleep(0)

        monkeypatch.setattr(managers.event_bus, "subscribe", subscribe)
        queue = asyncio.Queue()

        with caplog.at_level("ERROR"):
            task = asyncio.create_task(mqtt_manager.producer(queue))
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        assert queue.empty()
        assert "Malformed data received" in caplog.text

    @pytest.mark.asyncio
    async def test_consumer_publishes_json_payload(self, mqtt_manager, monkeypatch):
        """Publish queued events with QoS 2."""
        clients = []

        def fake_client_factory(**kwargs):
            client = FakeMqttClient(**kwargs)
            clients.append(client)
            return client

        monkeypatch.setattr(managers.aiomqtt, "Client", fake_client_factory)
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())
        queue = asyncio.Queue()
        payload = {"value": 12.5, "unit": "V"}
        queue.put_nowait(("sensors/test", payload))

        task = asyncio.create_task(mqtt_manager.consumer(queue, "worker", reconnect_interval=0))
        await asyncio.wait_for(queue.join(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        fake_client = clients[0]
        fake_client.publish.assert_awaited_once()
        assert fake_client.publish.await_args == call(
            "sensors/test",
            payload='{"value": 12.5, "unit": "V"}',
            qos=2,
        )
        assert fake_client.kwargs["hostname"] == "broker.example"
        assert fake_client.kwargs["port"] == 1883

    @pytest.mark.asyncio
    async def test_consumer_drops_unserializable_payload(self, mqtt_manager, monkeypatch, caplog):
        """Mark an event done when JSON serialization fails."""
        fake_client = FakeMqttClient()
        monkeypatch.setattr(managers.aiomqtt, "Client", lambda **kwargs: fake_client)
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())
        real_queue = asyncio.Queue()
        payload = {
            "timestamp": datetime(2100, 1, 1, 0, 0, 0, 0),
            "uuid": str(UUID("12345678-1234-5678-1234-567812345678")),
            "sid": 0,
            "value": Decimal("NaN"),
            "unit": "Hz",
        }
        real_queue.put_nowait(("sensors/test", payload))

        with caplog.at_level("DEBUG"):
            task = asyncio.create_task(mqtt_manager.consumer(real_queue, "worker", reconnect_interval=0))
            await asyncio.wait_for(real_queue.join(), timeout=1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        fake_client.publish.assert_not_awaited()
        assert "Error while serializing DataEvent" in caplog.text

    @pytest.mark.asyncio
    async def test_consumer_handles_connection_refused(self, mqtt_manager, monkeypatch, caplog):
        """Handle ConnectionRefusedError and retry the broker."""
        calls = 0

        class RefusingClient:
            """Client that refuses the first connection."""

            async def __aenter__(self):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise ConnectionRefusedError(errno.ECONNREFUSED, "Connection refused")
                raise asyncio.CancelledError

            async def __aexit__(self, exc_type, exc, traceback):
                return None

        monkeypatch.setattr(managers.aiomqtt, "Client", lambda **kwargs: RefusingClient())
        queue = asyncio.Queue()

        with caplog.at_level("ERROR"):
            task = asyncio.create_task(mqtt_manager.consumer(queue, "worker", reconnect_interval=0))
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=1)

        assert "Connection refused" in caplog.text

    @pytest.mark.asyncio
    async def test_cancel_tasks_delegates_to_helper(self, mqtt_manager, monkeypatch):
        """Delegate task cancellation to cancel_all_tasks."""
        cancel = AsyncMock()
        monkeypatch.setattr(managers, "cancel_all_tasks", cancel)
        tasks = {Mock()}

        await mqtt_manager.cancel_tasks(tasks)

        cancel.assert_awaited_once_with(tasks)

    @pytest.mark.asyncio
    async def test_cancel_tasks_logs_shutdown_errors(self, mqtt_manager, monkeypatch, caplog):
        """Log exceptions raised during task cancellation."""

        async def fail(_tasks):
            """Raise a test exception."""
            raise RuntimeError("shutdown failed")

        monkeypatch.setattr(managers, "cancel_all_tasks", fail)
        with caplog.at_level("ERROR"):
            await mqtt_manager.cancel_tasks(set())

        assert "Error during shutdown of the MQTT manager" in caplog.text


class TestDatabaseManager:
    """Tests for database manager lifecycle behavior."""

    def test_initializes_database_driver(self, monkeypatch):
        """Construct MongoDb with the supplied configuration."""
        database = Mock()
        mongo = Mock(return_value=database)
        monkeypatch.setattr(managers, "MongoDb", mongo)

        manager = managers.DatabaseManager(host="mongodb://localhost")

        mongo.assert_called_once_with(host="mongodb://localhost")
        assert manager._DatabaseManager__database_driver is database

    @pytest.mark.asyncio
    async def test_cancel_tasks_delegates_to_helper(self, monkeypatch):
        """Delegate database task cancellation to cancel_all_tasks."""
        database = managers.DatabaseManager(host="mongodb://localhost")
        cancel = AsyncMock()
        monkeypatch.setattr(managers, "cancel_all_tasks", cancel)
        tasks = {Mock()}

        await database.cancel_tasks(tasks)

        cancel.assert_awaited_once_with(tasks)

    @pytest.mark.asyncio
    async def test_cancel_tasks_logs_shutdown_errors(self, monkeypatch, caplog):
        """Log database shutdown failures."""
        database = managers.DatabaseManager(host="mongodb://localhost")

        async def fail(_tasks):
            """Raise a test exception."""
            raise RuntimeError("shutdown failed")

        monkeypatch.setattr(managers, "cancel_all_tasks", fail)
        with caplog.at_level("ERROR"):
            await database.cancel_tasks(set())

        assert "Error during shutdown of the DatabaseManager" in caplog.text


class TestHostManager:
    """Tests for host configuration and transport creation."""

    @pytest.mark.parametrize(
        "node_id,config,expected",
        [
            (None, {"enabled": True, "node_id": None}, True),
            (uuid4(), {"enabled": True, "node_id": None}, True),
            (None, {"enabled": True, "node_id": uuid4()}, True),
            (uuid4(), {"enabled": False, "node_id": None}, False),
        ],
    )
    def test_is_config_valid(self, node_id, config, expected):
        """Validate enabled and node-assigned configurations."""
        assert managers.HostManager._is_config_valid(node_id, config) is expected

    def test_is_config_valid_rejects_different_node(self):
        """Reject a configuration assigned to another node."""
        assert not managers.HostManager._is_config_valid(uuid4(), {"enabled": True, "node_id": uuid4()})

    def test_is_config_valid_rejects_none(self):
        """Reject a missing configuration."""
        assert not managers.HostManager._is_config_valid(uuid4(), None)

    def test_create_transport_returns_none_for_none(self):
        """Return no transport for an absent configuration."""
        assert managers.HostManager._create_transport(None) is None

    def test_create_transport_delegates_to_factory(self, monkeypatch):
        """Create a transport through the global factory."""
        transport = Mock()
        factory = Mock()
        factory.get.return_value = transport
        monkeypatch.setattr(managers, "transport_factory", factory)
        config = {"driver": "test", "port": 1234}

        assert managers.HostManager._create_transport(config) is transport
        factory.get.assert_called_once_with(**config)

    def test_create_transport_handles_unknown_driver(self, monkeypatch, caplog):
        """Return None and warn when the transport driver is unknown."""
        factory = Mock()
        factory.get.side_effect = UnknownDriverError("unknown")
        monkeypatch.setattr(managers, "transport_factory", factory)
        config = {"driver": "unknown"}

        with caplog.at_level("WARNING"):
            assert managers.HostManager._create_transport(config) is None

        assert "No driver available for transport 'unknown'" in caplog.text

    def test_create_transport_handles_driver_exception(self, monkeypatch, caplog):
        """Return None and log unexpected transport construction errors."""
        factory = Mock()
        factory.get.side_effect = RuntimeError("broken")
        monkeypatch.setattr(managers, "transport_factory", factory)
        config = {"driver": "broken"}

        with caplog.at_level("ERROR"):
            assert managers.HostManager._create_transport(config) is None

        assert "Error while creating transport 'broken'" in caplog.text

    def test_host_manager_stores_node_id(self):
        """Store the configured node UUID."""
        node_id = UUID("12345678-1234-5678-1234-567812345678")
        manager = managers.HostManager(node_id)

        assert manager._HostManager__node_id == node_id
        assert manager._HostManager__topic == "db_autodiscovery_sensors"
