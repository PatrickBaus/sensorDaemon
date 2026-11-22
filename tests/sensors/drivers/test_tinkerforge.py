"""Unit tests for the Tinkerforge sensor driver."""

# The tests intentionally exercise private helpers.
# pylint: disable=protected-access

import struct
from unittest.mock import AsyncMock, Mock

import pytest
from tinkerforge_async.bricklet_ptc import BrickletPtc
from tinkerforge_async.bricklet_temperature_v2 import BrickletTemperatureV2

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
