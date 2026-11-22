"""Unit tests for the Tinkerforge sensor driver."""

# The tests intentionally exercise private helpers.
# pylint: disable=protected-access

import struct
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest
from aiostream import pipe
from tinkerforge_async.bricklet_ptc import BrickletPtc
from tinkerforge_async.bricklet_ptc_v2 import BrickletPtcV2
from tinkerforge_async.bricklet_temperature_v2 import BrickletTemperatureV2
from tinkerforge_async.bricklet_thermocouple_v2 import BrickletThermocoupleV2
from tinkerforge_async.devices import AdvancedCallbackConfiguration, ThresholdOption

from sensors.drivers.tinkerforge import TinkerforgeSensor


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
