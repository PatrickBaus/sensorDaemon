"""Tests for the LabNode transport and sensor driver."""

# The tests intentionally exercise private helpers.
# pylint: disable=protected-access

import asyncio
from decimal import Decimal
from uuid import uuid4

import pytest
from aiostream import pipe, streamcontext

from async_event_bus import event_bus
from errors import SensorNotReady
from sensors.drivers.labnode import LabnodeSensor
from sensors.transports.labnode import LabnodeTransport


class FakeLabnode:
    """A LabNode, that returns increasing values and records the functions called."""

    def __init__(self) -> None:
        self.uuid = uuid4()
        self.calls: list[tuple[str, tuple]] = []
        self.__value = Decimal(0)

    def __str__(self) -> str:
        return "Fake LabNode"

    async def get_uuid(self):
        """The uuid of the LabNode"""
        return self.uuid

    async def get_by_function_id(self, function_id):
        """Read a value"""
        self.calls.append(("get_by_function_id", (function_id,)))
        self.__value += 1
        return self.__value

    async def set_kp(self, value):
        """A function, that can be called by the on_connect config"""
        self.calls.append(("set_kp", (value,)))


class FakeLabnodeTransport(LabnodeTransport):
    """A LabNode transport, that connects to a FakeLabnode instead of a real LabNode."""

    def __init__(self, device: FakeLabnode) -> None:
        super().__init__(hostname="192.0.2.1", port=4223, reconnect_interval=0, uuid=uuid4(), label="LabNode")
        self.device = device
        self.connected = False

    async def __aenter__(self):
        self.connected = True
        return self.device

    async def __aexit__(self, *_exc_info):
        self.connected = False


def labnode_config(uuid, enabled=True, on_connect=None):
    """The config of a LabNode with one sensor (sid 0), as it is served by the database."""
    return {
        "uuid": uuid,
        "uid": 1,
        "enabled": enabled,
        "label": "Test LabNode",
        "description": "",
        "config": {"0": {"interval": 0.01, "description": "", "topic": "sensors/labnode", "unit": "K", "timeout": 1}},
        "on_connect": on_connect or [],
    }


@pytest.fixture(name="labnode_database")
def labnode_database_fixture():
    """Serve the configs of LabNodes over the event bus, like the database does."""
    configs = {}

    async def get_config(uuid):
        return configs.get(uuid)

    event_bus.register("db_labnode_sensors/get_config", get_config)
    yield configs
    event_bus.unregister("db_labnode_sensors/get_config")


@pytest.mark.asyncio
async def test_the_transport_streams_the_data_of_the_labnode(labnode_database):
    """The transport connects, the LabNode is configured by its on_connect functions and its values are streamed."""
    device = FakeLabnode()
    labnode_database[device.uuid] = labnode_config(
        device.uuid, on_connect=[{"function": "set_kp", "args": [2], "kwargs": {}, "timeout": 1}]
    )
    transport = FakeLabnodeTransport(device)

    events = await asyncio.wait_for(transport.stream_data()[:2] | pipe.list(), timeout=3)

    assert [(event.sender, event.sid, event.topic, event.value, event.unit) for event in events] == [
        (device.uuid, 0, "sensors/labnode", Decimal(1), "K"),
        (device.uuid, 0, "sensors/labnode", Decimal(2), "K"),
    ]
    assert device.calls[0] == ("set_kp", (2,))
    assert not transport.connected  # disconnected, when the stream is closed
    assert (LabnodeTransport.driver(), transport.uri, transport.label) == ("labnode", "192.0.2.1:4223", "LabNode")


@pytest.mark.asyncio
async def test_a_labnode_without_config_waits_for_one(labnode_database):
    """
    A LabNode without config waits until a config is added. A disabled or removed config stops the sensor, a new
    config starts it again.
    """
    del labnode_database  # The database has no config for the LabNode
    device = FakeLabnode()
    sensor = LabnodeSensor(device)
    await sensor.enumerate()

    async with streamcontext(sensor.stream_data()) as data:
        reading = asyncio.ensure_future(anext(data))
        await asyncio.sleep(0.05)
        assert not reading.done()  # no config, no data

        event_bus.publish(f"nodes/by_uuid/{device.uuid}/add", labnode_config(device.uuid))
        assert (await asyncio.wait_for(reading, timeout=3)).value == Decimal(1)

        event_bus.publish(f"nodes/by_uuid/{device.uuid}/update", labnode_config(device.uuid, enabled=False))
        event_bus.publish(f"nodes/by_uuid/{device.uuid}/update", None)  # removed
        reading = asyncio.ensure_future(anext(data))
        await asyncio.sleep(0.05)
        reads = len(device.calls)
        await asyncio.sleep(0.05)
        assert len(device.calls) == reads  # the sensor is stopped

        event_bus.publish(f"nodes/by_uuid/{device.uuid}/update", labnode_config(device.uuid))
        assert (await asyncio.wait_for(reading, timeout=3)).sender == device.uuid


def test_a_config_with_an_unknown_on_connect_function_is_rejected(caplog):
    """A config, whose on_connect functions do not exist on the LabNode, is logged and ignored."""
    sensor = LabnodeSensor(FakeLabnode())
    config = labnode_config(
        uuid4(), on_connect=[{"function": "does_not_exist", "args": [], "kwargs": {}, "timeout": 1}]
    )
    with caplog.at_level("ERROR"):
        assert sensor._create_config(config) is None
    assert "Invalid configuration" in caplog.text
    assert sensor._create_config(None) is None


@pytest.mark.asyncio
async def test_a_sensor_must_be_enumerated_before_reading():
    """The uuid of the LabNode is needed for the data, so it must be enumerated first."""
    sensor = LabnodeSensor(FakeLabnode())
    with pytest.raises(SensorNotReady):
        sensor._read_sensor(0, interval=1, unit="K", topic="t", timeout=1)
    assert await (sensor._configure_and_stream(None) | pipe.list()) == []
    assert LabnodeSensor.driver() == "labnode"
