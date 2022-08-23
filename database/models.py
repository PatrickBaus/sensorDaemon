"""
This file contains all database data models, that represent either sensor
hosts/nodes or sensors.
"""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID, uuid4

import pymongo
from beanie import Document, PydanticObjectId
from pydantic import BaseModel, Field, validator  # pylint: disable=no-name-in-module  # <-- BaseModel


class FunctionCall(BaseModel):
    """
    Abstracted function call that can be called on a sensor.
    """

    # pylint: disable=too-few-public-methods
    function: str
    args: Optional[list] = []
    kwargs: Optional[dict] = {}
    timeout: Optional[float] = Field(ge=0)

    def execute(self, sensor) -> None:
        """
        Execute the function call on a sensor object.
        Parameters
        ----------
        sensor: TinkerforgeSensorModel
            A sensor that implements the function
        """
        getattr(sensor, self.function)(*self.args, **self.kwargs)


class HostBaseModel(BaseModel):
    """
    The base model all network hosts must implement.
    """

    # pylint: disable=too-few-public-methods
    hostname: int | str
    port: int = Field(ge=1, le=65535)
    pad: Optional[int] = Field(ge=0, le=30)
    sad: int  # Validator below
    driver: str
    node_id: Optional[UUID] = UUID("{00000000-0000-0000-0000-000000000000}")
    reconnect_interval: float | None = Field(ge=0)

    @validator("sad")
    @staticmethod
    def validate_all_fields_one_by_one(field_value):
        if field_value == 0 or 0x60 <= field_value <= 0x7E:
            return field_value
        raise ValueError("Invalid secondary address. Address must either be 0 or in the range (0x60, 0x7E)")

    class Settings:
        """
        The index, that makes sure, that a (host, port) tuple is unique.
        """

        indexes = [
            pymongo.IndexModel(
                [("hostname", pymongo.ASCENDING), ("port", pymongo.ASCENDING)],
                unique=True,
            )
        ]


class BaseDocument(Document):
    id: UUID = Field(default_factory=uuid4)


class TimeStampedDocument(BaseDocument):
    """
    A base class that implements a minimal audit trail by recording the
    creation date and the date of the last change.
    """

    # pylint: disable=too-few-public-methods
    date_created: datetime = datetime.utcnow()
    date_modified: datetime = datetime.utcnow()


class DeviceDocument(TimeStampedDocument):
    enabled: bool = True
    label: str | None
    description: Optional[str] = ""


class SensorHostModel(DeviceDocument, HostBaseModel):
    """
    An ethernet connected sensor host (inherited from the HostBaseModel).
    """

    # pylint: disable=too-few-public-methods

    class Settings:
        name = "SensorHost"


class TinkforgeSensorConfigModel(BaseModel):
    """
    The configuration of a sensor made by Tinkerforge GmbH.
    """

    # pylint: disable=too-few-public-methods
    interval: int = Field(ge=0, le=2**32 - 1)
    trigger_only_on_change: Optional[bool] = True
    description: Optional[str] = ""
    topic: str
    unit: PydanticObjectId | str


class TinkerforgeSensorModel(DeviceDocument):
    """
    The configuration of a sensor node, which is called a stack by Tinkerforge.
    """

    # pylint: disable=too-few-public-methods
    uid: int
    config: Dict[str, TinkforgeSensorConfigModel]  # bson does not allow int keys
    on_connect: List[FunctionCall] | List[None] = []

    class Settings:
        name = "TinkerforgeSensor"
        indexes = [
            pymongo.IndexModel(
                ("uid", pymongo.ASCENDING),
                unique=True,
            )
        ]


class LabnodeSensorConfigModel(BaseModel):
    """
    The configuration of a sensor made by Tinkerforge GmbH.
    """

    # pylint: disable=too-few-public-methods
    interval: int = Field(ge=0, le=2**32 - 1)
    description: Optional[str] = ""
    topic: str
    unit: PydanticObjectId | str
    timeout: Optional[float] = Field(ge=0)


class LabnodeSensorModel(DeviceDocument):
    uid: int
    config: Dict[str, LabnodeSensorConfigModel]  # bson does not allow int keys
    on_connect: List[FunctionCall] | List[None] = []

    class Settings:
        name = "LabnodeSensor"
        indexes = [
            pymongo.IndexModel(
                ("uid", pymongo.ASCENDING),
                unique=True,
            )
        ]


class GenericSensorModel(DeviceDocument):
    host: UUID
    driver: str
    interval: float = Field(ge=0)
    on_connect: List[FunctionCall] | List[None] = []
    on_read: FunctionCall
    on_after_read: List[FunctionCall] | List[None]
    on_disconnect: List[FunctionCall] | List[None] = []
    topic: str
    unit: str

    class Settings:
        name = "GenericSensor"
        indexes = [
            pymongo.IndexModel(
                ("host", pymongo.ASCENDING),
                unique=True,
            )
        ]
