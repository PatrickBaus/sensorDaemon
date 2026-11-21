"""Unit tests for the database models."""

import time
from datetime import UTC, datetime

from database.models import TimeStampedDocument


def test_timestamps_are_set_when_a_document_is_created():
    """date_created and date_modified default to the creation time, not to the time the module was imported."""
    first = TimeStampedDocument.model_construct()
    time.sleep(0.01)
    second = TimeStampedDocument.model_construct()

    assert first.date_created < second.date_created
    assert first.date_modified < second.date_modified
    assert abs((datetime.now(UTC) - second.date_created).total_seconds()) < 5
