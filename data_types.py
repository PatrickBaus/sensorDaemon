"""
This file contains all custom data types used across the application
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any
from uuid import UUID


class ChangeEvent:  # pylint: disable=too-few-public-methods
    """
    The base class to encapsulate any configuration/system change. The type of
    change is determined by inheritance.
    """

    @property
    def change(self) -> dict[str, Any] | UUID | None:
        """
        Returns
        -------
        dict or UUID or None
            The changed data.
        """
        return self.__change

    def __init__(self, change: dict[str, Any] | UUID | None) -> None:
        self.__change = change


@dataclass(frozen=True)
class DataEvent:
    """
    The base class to encapsulate any data event.
    """

    timestamp: float = field(init=False)
    sender: UUID
    sid: int
    topic: str
    value: Any
    unit: str

    def __post_init__(self):
        """Validate the event fields and initialize its timestamp."""
        if not isinstance(self.sender, UUID):
            raise TypeError(f"sender must be a UUID, not {type(self.sender).__name__}")
        if not isinstance(self.sid, int) or isinstance(self.sid, bool):
            raise TypeError(f"sid must be an int, not {type(self.sid).__name__}")
        if not isinstance(self.topic, str):
            raise TypeError(f"topic must be a str, not {type(self.topic).__name__}")
        if not isinstance(self.unit, str):
            raise TypeError(f"unit must be a str, not {type(self.unit).__name__}")

        # A slightly clumsy approach to setting the timestamp property, because this is frozen. Taken from:
        # https://docs.python.org/3/library/dataclasses.html#frozen-instances
        object.__setattr__(self, "timestamp", datetime.now(timezone.utc).timestamp())

    def __str__(self):
        return f"Data event from {self.sender}: {self.value} {self.unit}"


class ChangeType(Enum):
    """
    The type of changes sent out by the database.
    """

    ADD = auto()
    REMOVE = auto()
    UPDATE = auto()
