"""
This file contains all custom data types used across the application
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timezone, datetime
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

    def __init__(self, change:  dict[str, Any] | UUID | None) -> None:
        self.__change = change


class UpdateChangeEvent(ChangeEvent):   # pylint: disable=too-few-public-methods
    """
    An update to a configuration.
    """
    @property
    def change(self) -> dict[str, Any]:
        """
        Returns
        -------
        dict
            A dict with the changed configuration
        """
        return super().change


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
        # A slightly clumsy approach to setting the timestamp property, because this is frozen. Taken from:
        # https://docs.python.org/3/library/dataclasses.html#frozen-instances
        object.__setattr__(self, 'timestamp', datetime.now(timezone.utc).timestamp())

    def __str__(self):
        return f"Data event from {self.sender}: {self.value} {self.unit}"


class ChangeType(Enum):
    """
    The type of changes sent out by the database.
    """
    ADD = auto()
    REMOVE = auto()
    UPDATE = auto()
