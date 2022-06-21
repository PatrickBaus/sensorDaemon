"""
This file contains all database data models, that represent either sensor
hosts/nodes or sensors.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, List, Dict, Union
from uuid import UUID, uuid4

from beanie import Document, Indexed, PydanticObjectId
from pydantic import BaseModel, confloat, conint, Field  # pylint: disable=no-name-in-module
import pymongo


class FunctionCall(BaseModel):
    """
    Abstracted function call that can be called on a sensor.
    """
    # pylint: disable=too-few-public-methods
    function: str
    args: Optional[list] = []
    kwargs: Optional[dict] = {}
    timeout: Optional[confloat(ge=0)]

    def execute(self, sensor) -> None:
        """
        Execute the function call on a sensor object.
        Parameters
        ----------
        sensor: TinkerforgeSensor
            A sensor that implements the function
        """
        getattr(sensor, self.function)(*self.args, **self.kwargs)


class HostBaseModel(BaseModel):
    """
    The base model all network hosts must implement.
    """
    # pylint: disable=too-few-public-methods
    hostname: str
    port: conint(ge=1, le=65535)
    pad: Optional[conint(ge=0, le=30)]
    sad: Optional[Union[conint(ge=0x60, le=0x7E), conint(ge=0, le=0)]]
    driver: str
    node_id: Optional[UUID] = UUID('{00000000-0000-0000-0000-000000000000}')
    reconnect_interval: confloat(ge=0) | None

    class Settings:
        """
        The index, that makes sure, that a (host, port) tuple is unique.
        """
        indexes = [
            pymongo.IndexModel(
                [('hostname', pymongo.ASCENDING), ('port', pymongo.ASCENDING)],
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


class SensorHost(DeviceDocument, HostBaseModel):
    """
    An ethernet connected sensor host (inherited from the HostBaseModel).
    """
    # pylint: disable=too-few-public-methods


class TinkforgeSensorConfig(BaseModel):
    """
    The configuration of a sensor made by Tinkerforge GmbH.
    """
    # pylint: disable=too-few-public-methods
    interval: conint(ge=0, le=2**32-1)
    trigger_only_on_change: Optional[bool] = True
    description: Optional[str] = ""
    topic: str
    unit: PydanticObjectId | str


class TinkerforgeSensor(DeviceDocument):
    """
    The configuration of a sensor node, which is called a stack by Tinkerforge.
    """
    # pylint: disable=too-few-public-methods
    uid: Indexed(int, unique=True)
    config: Dict[str, TinkforgeSensorConfig]    # bson does not allow int keys
    on_connect: Union[List[FunctionCall], List[None]] = []


class LabnodeSensorConfig(BaseModel):
    """
    The configuration of a sensor made by Tinkerforge GmbH.
    """
    # pylint: disable=too-few-public-methods
    interval: conint(ge=0, le=2**32-1)
    description: Optional[str] = ""
    topic: str
    unit: PydanticObjectId | str


class LabnodeSensor(DeviceDocument):
    uid: Indexed(int, unique=True)
    config: Dict[str, LabnodeSensorConfig]    # bson does not allow int keys
    on_connect: Union[List[FunctionCall], List[None]] = []


class GenericSensor(DeviceDocument):
    host: Indexed(UUID, unique=True)
    driver: str
    interval: confloat(ge=0)
    on_connect: Union[List[FunctionCall], List[None]] = []
    on_read: FunctionCall
    on_after_read: Union[List[FunctionCall], List[None]]
    on_disconnect: Union[List[FunctionCall], List[None]] = []
    topic: str
    unit: str
