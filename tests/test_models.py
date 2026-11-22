"""Unit tests for the database models."""

import time
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from database.models import TimeStampedDocument, TinkerforgeSensorConfigModel


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
