"""Tests for the transports of generic devices (Ethernet, Prologix GPIB, Linux-GPIB) and the factories."""

# The tests intentionally exercise private helpers.
# pylint: disable=protected-access

import asyncio
from decimal import Decimal
from unittest.mock import Mock
from uuid import uuid4

import pytest
from aiostream import operator, pipe
from async_gpib import GpibError

from async_event_bus import event_bus
from data_types import DataEvent
from errors import UnknownDriverError
from sensors.drivers.hp3478a_driver import Hp3478ADriver
from sensors.factories.sensor_factory import sensor_factory
from sensors.transports.ethernet import EthernetTransport
from sensors.transports.generic_transport import GenericTransport
from sensors.transports.linux_gpib import LinuxGpibTransport
from sensors.transports.prologix_ethernet import PrologixEthernetTransport
from sensors.transports.transport_factory import transport_factory


class FakeScpiInstrument:
    """An SCPI instrument on a local TCP port that answers *IDN? and MEAS? and records the commands received."""

    def __init__(self) -> None:
        self.commands: list[str] = []
        self.server: asyncio.Server | None = None

    @property
    def port(self) -> int:
        """The TCP port the instrument listens on."""
        assert self.server is not None
        return self.server.sockets[0].getsockname()[1]

    async def __aenter__(self) -> "FakeScpiInstrument":
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self

    async def __aexit__(self, *_exc_info) -> None:
        assert self.server is not None
        self.server.close()
        await self.server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        replies = {"*IDN?": "ACME Instruments,Model 1,1234,1.0\n", "MEAS?": "1.5\n"}
        while line := await reader.readline():
            command = line.decode().strip()
            self.commands.append(command)
            if command in replies:
                writer.write(replies[command].encode())
                await writer.drain()
        writer.close()


class FakeGpibError(GpibError):
    """A GpibError that does not query the error code from the GPIB library, which is not installed for the tests."""

    def __init__(self, message: str) -> None:  # pylint: disable=super-init-not-called
        Exception.__init__(self, message)  # pylint: disable=non-parent-init-called


def scpi_sensor_config(host_uuid, sensor_uuid):
    """The config of a generic SCPI sensor as it is served by the database."""
    return {
        "uuid": sensor_uuid,
        "host": host_uuid,
        "driver": "generic_scpi2",
        "enabled": True,
        "label": "Test sensor",
        "description": "",
        "interval": 0.01,
        "on_connect": [],
        "on_read": {"function": "query_number", "args": ["MEAS?"], "kwargs": {}, "timeout": 1},
        "on_after_read": [],
        "on_disconnect": [],
        "topic": "sensors/test",
        "unit": "V",
    }


@pytest.fixture(name="generic_sensor_database")
def generic_sensor_database_fixture():
    """Serve the configs of generic sensors over the event bus, like the database does."""
    configs = {}

    async def get_config(host_uuid):
        return configs.get(host_uuid)

    event_bus.register("db_generic_sensors/get_config", get_config)
    yield configs
    event_bus.unregister("db_generic_sensors/get_config")


@pytest.mark.asyncio
async def test_ethernet_transport_streams_the_data_of_its_device(generic_sensor_database):
    """The transport connects, creates the device from the database config, and streams its measurements."""
    host_uuid, sensor_uuid = uuid4(), uuid4()
    generic_sensor_database[host_uuid] = scpi_sensor_config(host_uuid, sensor_uuid)

    async with FakeScpiInstrument() as instrument:
        transport = EthernetTransport(
            uuid=host_uuid, hostname="127.0.0.1", port=instrument.port, reconnect_interval=0, label="Test host"
        )
        events = await asyncio.wait_for(transport.stream_data()[:2] | pipe.list(), timeout=5)
        port = instrument.port

    assert all(isinstance(event, DataEvent) for event in events)
    assert [(event.sender, event.sid, event.topic, event.value, event.unit) for event in events] == [
        (sensor_uuid, 0, "sensors/test", Decimal("1.5"), "V")
    ] * 2
    assert instrument.commands[:3] == ["*IDN?", "MEAS?", "MEAS?"]
    assert (transport.name, transport.label, transport.uri) == ("Ethernet host", "Test host", f"127.0.0.1:{port}")
    assert EthernetTransport.driver() == "generic_ethernet2"


def test_create_device_without_config():
    """Without a config, there is no device."""
    assert GenericTransport._create_device(Mock(), None) == (None, None)


def test_create_device_with_an_unknown_driver(caplog):
    """A config with an unknown driver creates no device and logs a warning."""
    with caplog.at_level("WARNING"):
        assert GenericTransport._create_device(Mock(), {"driver": "unknown", "uuid": uuid4()}) == (None, None)
    assert "No driver available for device 'unknown'" in caplog.text


def test_create_device_with_a_faulty_driver(caplog, monkeypatch):
    """An error while creating a device is logged, so that the transport keeps listening for config updates."""
    monkeypatch.setattr(sensor_factory, "get", Mock(side_effect=RuntimeError("broken")))
    with caplog.at_level("ERROR"):
        assert GenericTransport._create_device(Mock(), {"driver": "generic_scpi2"}) == (None, None)
    assert "Error while creating device 'generic_scpi2'" in caplog.text


def failing_once(error: Exception):
    """A stream factory whose stream raises `error` on the first run and yields "data" on the second run."""
    runs = 0

    @operator
    async def generate():
        nonlocal runs
        runs += 1
        if runs == 1:
            raise error
        yield "data"

    def factory(*_args):
        # Like the streams of the transports, the stream can be iterated again, which restarts it
        return generate()

    return factory


@pytest.mark.asyncio
async def test_prologix_transport_restarts_the_device_after_invalid_data(monkeypatch):
    """A ValueError, e.g. caused by garbled data, restarts the device stream."""
    monkeypatch.setattr(GenericTransport, "_stream_data", failing_once(ValueError("garbled data")))
    transport = PrologixEthernetTransport(
        hostname="127.0.0.1", port=1234, pad=5, sad=0, reconnect_interval=0, uuid=uuid4(), label="Prologix"
    )

    assert await asyncio.wait_for(transport._stream_data(transport) | pipe.list(), timeout=1) == ["data"]
    assert (PrologixEthernetTransport.driver(), transport.uri) == ("prologix_gpib2", "127.0.0.1:1234")


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [FakeGpibError("bus error"), TimeoutError()])
async def test_linux_gpib_transport_reconnects_after_gpib_errors_and_timeouts(monkeypatch, error):
    """GPIB errors and timeouts (lost sync) restart the connection."""
    monkeypatch.setattr(LinuxGpibTransport, "_stream_data", failing_once(error))
    transport = LinuxGpibTransport(hostname="gpib0", pad=5, sad=0, reconnect_interval=0, uuid=uuid4(), label="GPIB")

    assert await asyncio.wait_for(transport.stream_data() | pipe.list(), timeout=1) == ["data"]
    assert (LinuxGpibTransport.driver(), transport.uri) == ("linux-gpib", "gpib0")


def test_sensor_factory():
    """The sensor factory knows the registered drivers and rejects unknown drivers."""
    assert sensor_factory.has("generic_scpi2")
    assert not sensor_factory.has("unknown")
    with pytest.raises(UnknownDriverError):
        sensor_factory.get("unknown", connection=Mock())


def test_transport_factory():
    """The transport factory creates the transports of the registered drivers and rejects unknown drivers."""
    transport = transport_factory.get(
        "generic_ethernet2", uuid=uuid4(), hostname="127.0.0.1", port=1234, reconnect_interval=1, label="Host"
    )
    assert isinstance(transport, EthernetTransport)
    with pytest.raises(UnknownDriverError):
        transport_factory.get("unknown")


def test_the_sensor_factory_creates_hp3478a_multimeters():
    """An HP 3478A multimeter is created with its own driver, which uses the connection of the transport."""
    connection = Mock()
    device = sensor_factory.get("hp3478a2", connection=connection, uuid=uuid4())

    assert isinstance(device, Hp3478ADriver)
    assert device.connection is connection
    assert Hp3478ADriver.driver() == "hp3478a2"
