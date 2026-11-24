"""Tests for the transport of Tinkerforge bricks and bricklets."""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from aiostream import pipe
from tinkerforge_async.bricklet_temperature_v2 import BrickletTemperatureV2, CallbackID
from tinkerforge_async.ip_connection import EnumerationType

from async_event_bus import event_bus
from sensors.drivers.tinkerforge import TinkerforgeSensor
from sensors.transports.tinkerforge import TinkerforgeTransport
from tests.fake_tinkerforge import FakeTinkerforgeConnection, tinkerforge_config


def test_properties():
    """The transport describes its host."""
    transport = TinkerforgeTransport("192.0.2.1", 4223, reconnect_interval=None, label="Stack")
    assert (transport.uri, transport.label, transport.reconnect_interval) == ("192.0.2.1:4223", "Stack", 1)
    assert TinkerforgeTransport.driver() == "tinkerforge2"


def test_only_the_first_enumeration_of_a_device_creates_a_sensor():
    """
    The first enumeration of a uid creates a sensor stream. Later enumerations of the same uid do not create a second
    one, but a CONNECTED enumeration restarts the existing sensor. Disconnected devices are not created.
    """
    device = BrickletTemperatureV2(12345, FakeTinkerforgeConnection())
    queues: dict = {}
    assert TinkerforgeTransport.route_enumeration(queues, (EnumerationType.DISCONNECTED, device)) is None
    assert not queues

    assert TinkerforgeTransport.route_enumeration(queues, (EnumerationType.AVAILABLE, device)) is not None
    assert queues[12345].qsize() == 1

    assert TinkerforgeTransport.route_enumeration(queues, (EnumerationType.AVAILABLE, device)) is None
    assert queues[12345].qsize() == 1  # a known device is not restarted, when it is only reported as available
    assert TinkerforgeTransport.route_enumeration(queues, (EnumerationType.CONNECTED, device)) is None
    assert queues[12345].qsize() == 2  # but it is restarted, when it was reset
    assert TinkerforgeTransport.route_enumeration(queues, (EnumerationType.DISCONNECTED, device)) is None
    assert queues[12345].qsize() == 2


@pytest.mark.asyncio
async def test_disconnected_devices_are_notified():
    """When a device is disconnected, its sensor is told to stop."""
    device = BrickletTemperatureV2(12345, FakeTinkerforgeConnection())
    removed = event_bus.subscribe("nodes/tinkerforge/12345/remove")
    waiting = asyncio.ensure_future(anext(removed))
    await asyncio.sleep(0)

    await TinkerforgeTransport.notify_device((EnumerationType.CONNECTED, device))  # no notification
    await TinkerforgeTransport.notify_device((EnumerationType.DISCONNECTED, device))

    assert await asyncio.wait_for(waiting, timeout=1) is None
    await removed.aclose()


@pytest.mark.asyncio
async def test_the_transport_streams_the_data_of_the_devices_it_finds(tinkerforge_database, monkeypatch):
    """The transport connects, enumerates the devices, creates a sensor for each new device, and streams its data."""
    connection = FakeTinkerforgeConnection()
    device = BrickletTemperatureV2(12345, connection)
    sensor_uuid = uuid4()
    tinkerforge_database[12345] = tinkerforge_config(12345, sensor_uuid)

    transport = TinkerforgeTransport("192.0.2.1", 4223, reconnect_interval=0, label="Stack")
    connect, disconnect, enumerate_devices = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(transport, "connect", connect)
    monkeypatch.setattr(transport, "disconnect", disconnect)
    monkeypatch.setattr(transport, "enumerate", enumerate_devices)

    async def read_enumeration():
        yield EnumerationType.CONNECTED, device
        await asyncio.Event().wait()

    monkeypatch.setattr(transport, "read_enumeration", read_enumeration)

    connection.callback(device, CallbackID.TEMPERATURE, "h", 2315)
    events = await asyncio.wait_for(transport.stream_data()[:1] | pipe.list(), timeout=3)

    assert [(event.sender, event.value) for event in events] == [(sensor_uuid, Decimal("296.30"))]
    connect.assert_awaited_once()
    enumerate_devices.assert_awaited_once()
    disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_reset_device_is_configured_again(tinkerforge_database, monkeypatch):
    """
    A bricklet that is reset sends a CONNECTED enumeration while its sensor is still running. Regression test for
    https://github.com/PatrickBaus/sensorDaemon/issues/363: this created a second sensor with the same uid, which failed
    with an EventRegisteredError and restarted the whole transport. Instead, the sensor must be replaced.
    """
    connection = FakeTinkerforgeConnection()
    device = BrickletTemperatureV2(12345, connection)
    sensor_uuid = uuid4()
    tinkerforge_database[12345] = tinkerforge_config(12345, sensor_uuid)

    transport = TinkerforgeTransport("192.0.2.1", 4223, reconnect_interval=0, label="Stack")
    monkeypatch.setattr(transport, "connect", AsyncMock())
    monkeypatch.setattr(transport, "disconnect", AsyncMock())
    monkeypatch.setattr(transport, "enumerate", AsyncMock())
    enumerations: asyncio.Queue = asyncio.Queue()

    async def read_enumeration():
        while True:
            yield await enumerations.get()

    monkeypatch.setattr(transport, "read_enumeration", read_enumeration)
    configured = []
    original_configure = TinkerforgeSensor._configure  # pylint: disable=protected-access

    async def configure(self, config):
        configured.append(self)
        return await original_configure(self, config)

    monkeypatch.setattr(TinkerforgeSensor, "_configure", configure)

    async def run():
        async with transport.stream_data().stream() as streamer:
            async for _ in streamer:
                pass

    enumerations.put_nowait((EnumerationType.AVAILABLE, device))
    enumerations.put_nowait((EnumerationType.CONNECTED, device))  # the bricklet was reset during the enumeration
    task = asyncio.create_task(run())
    try:
        async with asyncio.timeout(3):
            while len(configured) < 1:
                await asyncio.sleep(0.01)
            enumerations.put_nowait((EnumerationType.CONNECTED, device))  # the bricklet is reset again
            while len(configured) < 2:
                await asyncio.sleep(0.01)
        await asyncio.sleep(0.1)
        assert not task.done()  # the transport was not torn down by an error
        assert len(set(configured)) == len(configured)  # each reset created a new sensor
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
