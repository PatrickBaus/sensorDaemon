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
from sensors.transports.tinkerforge import TinkerforgeTransport
from tests.fake_tinkerforge import FakeTinkerforgeConnection, tinkerforge_config


def test_properties():
    """The transport describes its host."""
    transport = TinkerforgeTransport("192.0.2.1", 4223, reconnect_interval=None, label="Stack")
    assert (transport.uri, transport.label, transport.reconnect_interval) == ("192.0.2.1:4223", "Stack", 1)
    assert TinkerforgeTransport.driver() == "tinkerforge2"


@pytest.mark.asyncio
async def test_new_and_unknown_devices_are_created():
    """
    A device is created, if it was connected, or if it is reported as available and not yet known. Disconnected
    devices and known devices are not created again.
    """
    device = BrickletTemperatureV2(12345, FakeTinkerforgeConnection())
    assert await TinkerforgeTransport.filter_enumerations((EnumerationType.CONNECTED, device))
    assert await TinkerforgeTransport.filter_enumerations((EnumerationType.AVAILABLE, device))
    assert not await TinkerforgeTransport.filter_enumerations((EnumerationType.DISCONNECTED, device))

    async def status():
        """The status of a known device"""

    event_bus.register("nodes/tinkerforge/12345/status", status)
    try:
        assert not await TinkerforgeTransport.filter_enumerations((EnumerationType.AVAILABLE, device))
    finally:
        event_bus.unregister("nodes/tinkerforge/12345/status")


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
