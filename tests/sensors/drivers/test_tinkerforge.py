"""Unit tests for the Tinkerforge sensor driver."""

# The tests intentionally exercise private helpers.
# pylint: disable=protected-access

import asyncio
import struct
from decimal import Decimal
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from aiostream import pipe, streamcontext
from tinkerforge_async.bricklet_isolator import BrickletIsolator
from tinkerforge_async.bricklet_ptc import BrickletPtc
from tinkerforge_async.bricklet_ptc_v2 import BrickletPtcV2
from tinkerforge_async.bricklet_temperature_v2 import BrickletTemperatureV2, CallbackID
from tinkerforge_async.bricklet_thermocouple_v2 import BrickletThermocoupleV2
from tinkerforge_async.devices import AdvancedCallbackConfiguration, SimpleCallbackConfiguration, ThresholdOption
from tinkerforge_async.ip_connection_helper import pack_payload

from async_event_bus import TopicNotRegisteredError, event_bus
from sensors.drivers.shared import DeviceStatus
from sensors.drivers.tinkerforge import TinkerforgeSensor
from tests.fake_tinkerforge import FakeTinkerforgeConnection, tinkerforge_config


def fake_ip_connection():
    """An IP connection, that records the requests sent and returns an empty reply."""
    ipcon = Mock()
    ipcon.send_request = AsyncMock(return_value=(None, b""))
    return ipcon


def test_interval_is_passed_as_period_in_s():
    """The database stores the interval in s, like tinkerforge_async expects the callback period."""
    sid, unit, topic, callback_config = TinkerforgeSensor._parse_callback_configuration(
        "1", {"interval": 1.5, "trigger_only_on_change": False, "unit": "K", "topic": "sensors/test"}
    )

    assert (sid, unit, topic) == (1, "K", "sensors/test")
    assert callback_config.period == 1.5
    assert callback_config.value_has_to_change is False


@pytest.mark.asyncio
@pytest.mark.parametrize("device_class", [BrickletTemperatureV2, BrickletPtc])
async def test_the_device_receives_the_interval_in_ms(device_class):
    """An interval of 1 s in the database is sent to the device as 1000 ms."""
    device = device_class(12345, fake_ip_connection())
    sid, _, _, callback_config = TinkerforgeSensor._parse_callback_configuration(
        "0", {"interval": 1, "trigger_only_on_change": True, "unit": "K", "topic": "sensors/test"}
    )

    await device.set_callback_configuration(sid, *callback_config)

    # The first request sets the period. Its payload starts with the period in ms as an uint32.
    request = device.ipcon.send_request.await_args_list[0].kwargs
    assert struct.unpack_from("<I", request["data"])[0] == 1000


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "device_class,sid",
    [
        (BrickletPtcV2, 5),  # does not exist
        (BrickletPtcV2, 2),  # sensor connected, has no callback configuration
        (BrickletThermocoupleV2, 1),  # error state, has no callback configuration
    ],
)
async def test_invalid_sid_disables_only_this_sensor(device_class, sid, caplog):
    """An invalid sid in the database config disables the sensor instead of stopping the transport."""
    device = device_class(12345, fake_ip_connection())
    sensor = TinkerforgeSensor(device)
    callback_config = AdvancedCallbackConfiguration(
        period=Decimal(1), value_has_to_change=True, option=ThresholdOption.OFF, minimum=None, maximum=None
    )

    with caplog.at_level("ERROR"):
        result = await sensor._set_callback_configuration(sid, "K", "sensors/test", callback_config)

    assert await (result | pipe.list()) == []
    device.ipcon.send_request.assert_not_awaited()  # Nothing was sent to the device
    assert f"Invalid configuration: Invalid sid {sid} for {device}." in caplog.text


@pytest.mark.asyncio
async def test_a_configuration_without_threshold_option_is_applied():
    """A configuration read back from a device has no option (None) if the channel has no thresholds."""
    device = BrickletTemperatureV2(12345, fake_ip_connection())
    config = AdvancedCallbackConfiguration(
        period=Decimal("0.5"), value_has_to_change=True, option=None, minimum=None, maximum=None
    )

    await TinkerforgeSensor(device)._apply_callback_configuration(0, config)

    # Payload: period (uint32, ms), value_has_to_change (bool), option (char), minimum, maximum
    request = device.ipcon.send_request.await_args_list[0].kwargs
    period, value_has_to_change, option = struct.unpack_from("<I?c", request["data"])
    assert (period, value_has_to_change, option) == (500, True, ThresholdOption.OFF.value.encode())


@pytest.mark.asyncio
async def test_a_simple_configuration_is_applied():
    """The Isolator Bricklet returns a SimpleCallbackConfiguration, which has no thresholds."""
    device = BrickletIsolator(12345, fake_ip_connection())

    await TinkerforgeSensor(device)._apply_callback_configuration(
        0, SimpleCallbackConfiguration(period=Decimal(2), value_has_to_change=False)
    )

    request = device.ipcon.send_request.await_args_list[0].kwargs
    assert struct.unpack_from("<I?", request["data"]) == (2000, False)


async def wait_for(condition, timeout=3.0):
    """Wait until `condition()` is true."""
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_a_bricklet_is_configured_and_streams_its_data(tinkerforge_database):
    """
    The bricklet is configured by the on_connect functions and the callback configuration of the database. Its
    callbacks are streamed. A removed config stops the sensor, a new config starts it again, and the sensor stops,
    when the bricklet is disconnected.
    """
    connection = FakeTinkerforgeConnection()
    device = BrickletTemperatureV2(12345, connection)
    sensor_uuid = uuid4()
    tinkerforge_database[12345] = tinkerforge_config(
        12345,
        sensor_uuid,
        on_connect=[{"function": "set_heater_configuration", "args": [1], "kwargs": {}, "timeout": 1}],
    )

    async with streamcontext(TinkerforgeSensor(device).stream_data()) as data:
        connection.callback(device, CallbackID.TEMPERATURE, "h", 2315)  # 23.15 °C
        event = await asyncio.wait_for(anext(data), timeout=3)
        assert (event.sender, event.sid, event.topic, event.unit) == (sensor_uuid, 0, "sensors/temperature", "K")
        assert event.value == Decimal("296.30")
        assert connection.names_of_requests()[:2] == [
            "SET_HEATER_CONFIGURATION",  # on_connect
            "SET_TEMPERATURE_CALLBACK_CONFIGURATION",
        ]
        # While the sensor runs, it answers status requests
        assert await event_bus.call("nodes/tinkerforge/12345/status") == DeviceStatus()

        # The config is removed from the database, then a new config is added
        event_bus.publish(f"nodes/by_uuid/{sensor_uuid}/remove", None)
        new_uuid = uuid4()
        event_bus.publish("nodes/tinkerforge/12345/update", tinkerforge_config(12345, new_uuid, interval=2))
        # The stream only runs while it is read
        reading = asyncio.ensure_future(anext(data))
        await wait_for(lambda: connection.names_of_requests().count("SET_TEMPERATURE_CALLBACK_CONFIGURATION") == 2)
        connection.callback(device, CallbackID.TEMPERATURE, "h", 2400)
        event = await asyncio.wait_for(reading, timeout=3)
        assert (event.sender, event.value) == (new_uuid, Decimal("297.15"))

        # The bricklet is disconnected
        reading = asyncio.ensure_future(anext(data))
        await asyncio.sleep(0.01)
        event_bus.publish("nodes/tinkerforge/12345/remove", None)
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(reading, timeout=3)

    with pytest.raises(TopicNotRegisteredError):
        await event_bus.call("nodes/tinkerforge/12345/status")


@pytest.mark.asyncio
@pytest.mark.slow
async def test_the_callback_configuration_is_restored_after_a_reset_of_the_bricklet(tinkerforge_database, caplog):
    """The callback configuration is checked every second and set again, if it was lost, e.g. by a reset."""
    connection = FakeTinkerforgeConnection()
    device = BrickletTemperatureV2(12345, connection)
    tinkerforge_database[12345] = tinkerforge_config(12345, uuid4())

    with caplog.at_level("INFO"):
        async with streamcontext(TinkerforgeSensor(device).stream_data()) as data:
            reading = asyncio.ensure_future(anext(data))
            await wait_for(lambda: "TEMPERATURE_CALLBACK_CONFIGURATION" in connection.callback_configurations)
            configured = connection.callback_configurations["TEMPERATURE_CALLBACK_CONFIGURATION"]
            # The bricklet was reset. It reports its default configuration: period 0, no thresholds.
            connection.callback_configurations["TEMPERATURE_CALLBACK_CONFIGURATION"] = pack_payload(
                (0, False, b"x", 0, 0), "I ! c h h"
            )
            await wait_for(
                lambda: connection.callback_configurations["TEMPERATURE_CALLBACK_CONFIGURATION"] == configured
            )
            reading.cancel()

    assert "Resetting callback config" in caplog.text


@pytest.mark.asyncio
async def test_a_config_with_an_unknown_on_connect_function_is_ignored(tinkerforge_database, caplog):
    """A config, whose on_connect functions do not exist, is logged and the bricklet is not configured."""
    connection = FakeTinkerforgeConnection()
    device = BrickletTemperatureV2(12345, connection)
    tinkerforge_database[12345] = tinkerforge_config(
        12345, uuid4(), on_connect=[{"function": "does_not_exist", "args": [], "kwargs": {}, "timeout": 1}]
    )

    with caplog.at_level("ERROR"):
        async with streamcontext(TinkerforgeSensor(device).stream_data()) as data:
            reading = asyncio.ensure_future(anext(data))
            await wait_for(lambda: "Invalid configuration" in caplog.text)
            reading.cancel()

    assert not connection.requests


@pytest.mark.asyncio
async def test_a_sensor_rejected_by_the_device_is_disabled(caplog):
    """If the device does not accept the callback period (it reads back 0), the sensor is disabled."""
    device = BrickletTemperatureV2(12345, fake_ip_connection())
    device.get_callback_configuration = AsyncMock(
        return_value=AdvancedCallbackConfiguration(Decimal(0), False, ThresholdOption.OFF, None, None)
    )
    callback_config = AdvancedCallbackConfiguration(Decimal(1), False, ThresholdOption.OFF, None, None)

    with caplog.at_level("WARNING"):
        result = await TinkerforgeSensor(device)._set_callback_configuration(0, "K", "t", callback_config)

    assert await (result | pipe.list()) == []
    assert "failed. Source disabled" in caplog.text


@pytest.mark.asyncio
async def test_without_a_config_there_is_nothing_to_stream():
    """Without a config, a sensor is not configured and streams nothing."""
    sensor = TinkerforgeSensor(BrickletTemperatureV2(12345, fake_ip_connection()))
    assert sensor._create_config(None) is None
    assert await (sensor._configure_and_stream(None) | pipe.list()) == []


def test_driver_name():
    """The driver is identified by its name."""
    assert TinkerforgeSensor.driver() == "tinkerforge2"
