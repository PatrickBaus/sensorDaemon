# -*- coding: utf-8 -*-
"""
This file contains all database data models, that represent either sensor
hosts/nodes or sensors.
"""
from datetime import datetime
from typing import Optional, List, Dict, Union
from uuid import UUID, uuid4

from beanie import Document, Indexed, PydanticObjectId
from pydantic import BaseModel, conint, Field   # pylint: disable=no-name-in-module
import pymongo


class FunctionCall(BaseModel):
    """
    Abstracted function call that can be called on a sensor.
    """
    # pylint: disable=too-few-public-methods
    function: str
    args: Optional[List[int]] = []
    kwargs: Optional[dict] = {}

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
    driver: str

    class Collection:
        """
        The index, that makes sure, that a (host, port) tuple is unique.
        """
        indexes = [
            pymongo.IndexModel(
                [('hostname', pymongo.ASCENDING), ('port', pymongo.ASCENDING)],
                unique=True,
            )
        ]


class TimeStampedDocument(Document):
    """
    A base class that implements a minimal audit trail by recording the
    creation date and the the date of the last cahnge.
    """
    # pylint: disable=too-few-public-methods
    date_created: datetime = datetime.utcnow()
    date_modified: datetime = datetime.utcnow()


class SensorHost(TimeStampedDocument, HostBaseModel):
    """
    An ethernet connected sensor host.
    """
    # pylint: disable=too-few-public-methods
    id: UUID = Field(default_factory=uuid4)
    label: str
    description: Optional[str] = ""


class Sensor(TimeStampedDocument):
    id: UUID = Field(default_factory=uuid4)

class SensorUnit(Document):
    """
    The (SI) unit of the sensor output.
    """
    # pylint: disable=too-few-public-methods
    label: Indexed(str, unique=True)


class TinkforgeSensorConfig(BaseModel):
    """
    The configuration of a sensor made by Tinkerforge GmbH.
    """
    # pylint: disable=too-few-public-methods
    interval: conint(ge=0, le=2**32-1)
    trigger_only_on_change: Optional[bool] = True
    description: Optional[str] = ""
    topic: str
    unit: PydanticObjectId


class TinkerforgeSensor(Sensor):
    """
    The configuration of a sensor node, which is called a stack by Tinkerforge.
    """
    # pylint: disable=too-few-public-methods
    uid: Indexed(int, unique=True)
    config: Dict[str, TinkforgeSensorConfig]    # bson does not allow int keys
    on_connect: Union[List[FunctionCall], List[None]] = []


class GpibSensor(Sensor):
    """
    The configuration of a GPIB connector.
    """
    # pylint: disable=too-few-public-methods
    label: str
    pad: conint(ge=0, le=30)
    sad: Optional[Union[conint(ge=0x60, le=0x7E), conint(ge=0, le=0)]] = 0
    driver: str
    interval: conint(ge=0)
    on_read: FunctionCall
    on_connect: Union[List[FunctionCall], List[None]]
    before_read: Union[List[FunctionCall], List[None]]
    after_read: Union[List[FunctionCall], List[None]]
    topic: str
    unit: PydanticObjectId
    host: Indexed(UUID)

    class Collection:
        """
        The index, that makes sure, that a (host, port) tuple is unique.
        """
        indexes = [
            pymongo.IndexModel(
                [('pad', pymongo.ASCENDING), ('sad', pymongo.ASCENDING), ('host', pymongo.ASCENDING)],
                unique=True,
            )
        ]
