"""Unit tests for the DataEvent type."""

from uuid import UUID

import pytest

from data_types import DataEvent


class TestDataEvent:
    """Test DataEvent validation and behavior."""

    def test_data_event_shape(self):
        """Test the DataEvent fields and generated timestamp."""
        sender = UUID("12345678-1234-5678-1234-567812345678")
        event = DataEvent(sender=sender, topic="temperature", value=12.5, sid=3, unit="K")
        assert event.sender == sender
        assert event.topic == "temperature"
        assert event.value == 12.5
        assert event.sid == 3
        assert event.unit == "K"
        assert event.timestamp > 0

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("sender", "not-a-uuid"),
            ("sid", "not-an-int"),
            ("topic", 123),
            ("unit", 123),
        ],
    )
    def test_data_event_rejects_invalid_types(self, field, value):
        """Test that DataEvent rejects values with invalid types."""
        values = {
            "sender": UUID("12345678-1234-5678-1234-567812345678"),
            "sid": 3,
            "topic": "temperature",
            "value": 12.5,
            "unit": "K",
        }
        values[field] = value

        with pytest.raises(TypeError, match=field):
            DataEvent(**values)

    def test_data_event_accepts_any_value_type(self):
        """Test that the value field intentionally accepts arbitrary types."""
        event = DataEvent(
            sender=UUID("12345678-1234-5678-1234-567812345678"),
            topic="status",
            value={"enabled": True},
            sid=3,
            unit="",
        )

        assert event.value == {"enabled": True}
