"""Unit tests for the database models."""

import time
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from database.models import FunctionCall, HostBaseModel, TimeStampedDocument, TinkerforgeSensorConfigModel


def test_timestamps_are_set_when_a_document_is_created():
    """date_created and date_modified default to the creation time, not to the time the module was imported."""
    first = TimeStampedDocument.model_construct()
    time.sleep(0.01)
    second = TimeStampedDocument.model_construct()

    assert first.date_created < second.date_created
    assert first.date_modified < second.date_modified
    assert abs((datetime.now(UTC) - second.date_created).total_seconds()) < 5


def test_tinkerforge_interval_is_given_in_s():
    """The interval of a Tinkerforge sensor is a duration in s, so fractions of a second are allowed."""
    config = TinkerforgeSensorConfigModel(interval=0.25, topic="sensors/test", unit="K")
    assert config.interval == 0.25
    # The devices store the period as uint32 in ms
    with pytest.raises(ValidationError):
        TinkerforgeSensorConfigModel(interval=4294967.296, topic="sensors/test", unit="K")


def test_function_call_is_executed_on_the_sensor():
    """A stored function call calls the named function of the sensor with its arguments."""
    sensor = Mock()
    FunctionCall(function="set_moving_average", args=[1, 2], kwargs={"enabled": True}, timeout=1).execute(sensor)
    sensor.set_moving_average.assert_called_once_with(1, 2, enabled=True)


@pytest.mark.parametrize("sad", [None, 0, 0x60, 0x7E])
def test_valid_secondary_gpib_addresses(sad):
    """The secondary address is either disabled (None or 0) or in the range 0x60 to 0x7E."""
    host = HostBaseModel(hostname="gpib0", port=1234, pad=1, sad=sad, driver="linux_gpib", reconnect_interval=1)
    assert host.sad == sad


@pytest.mark.parametrize("sad", [1, 0x5F, 0x7F])
def test_invalid_secondary_gpib_addresses(sad):
    """Secondary addresses outside of the range 0x60 to 0x7E are rejected."""
    with pytest.raises(ValidationError, match="Invalid secondary address"):
        HostBaseModel(hostname="gpib0", port=1234, pad=1, sad=sad, driver="linux_gpib", reconnect_interval=1)
