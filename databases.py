"""
The database abstraction layer, that handles all application specific tasks of
the databases.
"""
from __future__ import annotations

import asyncio
import logging
from types import TracebackType
from typing import Any, AsyncGenerator, Type

try:
    from typing import Self  # type: ignore # Python 3.11
except ImportError:
    from typing_extensions import Self

from uuid import UUID

import beanie
import motor.motor_asyncio
import pymongo  # to access the error classes
from beanie import init_beanie

from async_event_bus import event_bus
from data_types import ChangeType
from database.models import (
    BaseDocument,
    DeviceDocument,
    GenericSensorModel,
    LabnodeSensorModel,
    SensorHostModel,
    TinkerforgeSensorModel,
)


class MongoDb:
    """
    The Mongo DB abstraction for the settings database.
    """

    def __init__(self, hostname: str = None, port: int = None) -> None:
        self.__hostname = hostname
        self.__port = port
        self.__client = None
        self.__logger = logging.getLogger(__name__)

    async def __aenter__(self) -> Self:
        await self.__connect()
        return self

    async def __aexit__(
        self, exc_type: Type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        pass

    async def __connect(self):
        """
        Connect to the database. Retries if not successful.
        """
        connection_attempt = 1
        timeout = 0.5  # in s TODO: Make configurable
        while "database not connected":
            hostname_string = self.__hostname if self.__port is None else f"{self.__hostname}:{self.__port}"
            if connection_attempt == 1:
                self.__logger.info("Connecting to MongoDB (%s).", hostname_string[hostname_string.find("@") + 1 :])
            if self.__port is not None:
                self.__client = motor.motor_asyncio.AsyncIOMotorClient(
                    self.__hostname, self.__port, serverSelectionTimeoutMS=timeout * 1000
                )
            else:
                self.__client = motor.motor_asyncio.AsyncIOMotorClient(
                    self.__hostname,
                    serverSelectionTimeoutMS=timeout * 1000,
                )

            database = self.__client.sensor_config
            try:
                await init_beanie(
                    database=database,
                    document_models=[
                        BaseDocument,
                        SensorHostModel,
                        TinkerforgeSensorModel,
                        LabnodeSensorModel,
                        GenericSensorModel,
                    ],
                )
                self.__logger.info("MongoDB (%s) connected.", hostname_string[hostname_string.find("@") + 1 :])
            except pymongo.errors.ServerSelectionTimeoutError as exc:
                if connection_attempt == 1:
                    # Only log the error once
                    self.__logger.error(
                        "Cannot connect to config database at %s. Error: %s. Retrying in %f s.",
                        hostname_string,
                        exc,
                        timeout,
                    )
                await asyncio.sleep(timeout)
                continue
            finally:
                connection_attempt += 1
            break


class Context:  # pylint: disable=too-few-public-methods
    """
    The database context used by all config databases.
    """

    @property
    def topic(self) -> str:
        """
        Returns
        -------
        str
            The event_bus topic the context is registered to.
        """
        return self.__topic

    def __init__(self, topic: str) -> None:
        self.__topic = topic
        self.__logger = logging.getLogger(__name__)

    async def __aenter__(self) -> Self:
        raise NotImplementedError

    async def __aexit__(
        self, exc_type: Type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        raise NotImplementedError

    async def _monitor_database(
        self, database_model: Type[DeviceDocument], timeout: float
    ) -> AsyncGenerator[tuple[ChangeType, UUID | beanie.Document], None]:
        """
        Monitor all changes made to a certain database table.

        Parameters
        ----------
        database_model: Type[DeviceDocument]
            The database model of the table to be monitored
        timeout: float
            The timeout in seconds to wait after a connection error.

        Yields
        -------
        tuple of ChangeType and UUID or beanie.Document
            An iterator that yields the type of change and either the new DeviceDocument or the UUID of the deleted
            device
        """
        resume_token = None
        # To watch only for certain events, use:
        # pipeline = [{'$match': {'operationType': {'$in': ["insert", "update", "delete"]}}}]
        # In a Mongo DB the __id cannot be updated. It can only be removed and recreated. It is therefore safe to link
        # all configs to this uuid.
        pipeline: list[dict[str, dict[str, str]]] = []
        while "loop not cancelled":
            try:
                async with database_model.get_motor_collection().watch(
                    pipeline, full_document="updateLookup", resume_after=resume_token
                ) as change_stream:
                    async for change in change_stream:
                        # TODO: catch parser errors!
                        if change["operationType"] == "delete":
                            yield ChangeType.REMOVE, change["documentKey"]["_id"]
                        elif change["operationType"] == "update" or change["operationType"] == "replace":
                            yield ChangeType.UPDATE, database_model.parse_obj(change["fullDocument"])
                        elif change["operationType"] == "insert":
                            yield ChangeType.ADD, database_model.parse_obj(change["fullDocument"])

                    resume_token = change_stream.resume_token
            except pymongo.errors.ServerSelectionTimeoutError as exc:
                self.__logger.error(
                    "Connection error while monitoring config database. Error: %s. Reconnecting in %f s.", exc, timeout
                )
                await asyncio.sleep(timeout)
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                if resume_token is None:
                    # There is no usable resume token because there was a
                    # failure during ChangeStream initialization.
                    self.__logger.exception(
                        "Cannot resume Mongo DB change stream, there is no token. Starting from scratch."
                    )

                await asyncio.sleep(timeout)


class LabnodeContext(Context):
    """
    The Labnode configuration database context manager. It monitors changes
    to the database and publishes them onto the event bus. It also provides an
    endpoint to query for sensor configs via the event bus.
    """

    def __init__(self):
        super().__init__(topic="db_labnode_sensors")
        self.__logger = logging.getLogger(__name__)

    async def __aenter__(self) -> Self:
        event_bus.register(self.topic + "/get_config", self.__get_sensor_config)
        event_bus.publish(self.topic + "/status_update", True)  # TODO: use a proper event
        return self

    async def __aexit__(
        self, exc_type: Type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        event_bus.unregister(self.topic + "/get_config")

    async def __get_sensor_config(self, uuid: UUID) -> dict[str, Any] | None:
        """
        Get all host configs from the database.

        Parameters
        ----------
        uuid: UUID
            The device uuid

        Returns
        -------
        dict
            A dictionary, that contains the configuration of the sensor.
        """
        try:
            device = await LabnodeSensorModel.find_one(LabnodeSensorModel.id == uuid)
        except (ValueError, pymongo.errors.ServerSelectionTimeoutError) as exc:
            # If the pydantic validation fails, we get a ValueError
            self.__logger.error("Error while getting configuration for Labnode device %s: %s", uuid, exc)
            device = None

        if device is None:
            return device

        device = device.dict()
        # Rename the id key, because, the id is called uuid throughout the program. Note: This moves the uuid field to
        # the back of the dict
        device["uuid"] = device.pop("id")

        return device

    async def monitor_changes(self, timeout: float) -> None:
        """
        Push changes from the database onto the event_bus.

        Parameters
        ----------
        timeout: float
            The timeout in seconds to wait after a connection error.
        """
        change: beanie.Document
        async for change_type, change in self._monitor_database(LabnodeSensorModel, timeout):
            # Remember: Do not await in the iterator, as this stops the stream of updates
            if change_type == ChangeType.UPDATE:
                change_dict = change.dict()
                # Rename the id key, because we use the parameter uuid throughout the program, because `id` is already
                # used in Python
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['uuid']}/update", change_dict)
            elif change_type == ChangeType.ADD:
                change_dict = change.dict()
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['host']}/add", change_dict)
            elif change_type == ChangeType.REMOVE:
                # When removing sensors, the DB only returns the uuid
                event_bus.publish(f"nodes/by_uuid/{change_dict}/update", None)


class GenericSensorContext(Context):
    """
    The ethernet configuration database context manager. It monitors changes
    to the database and publishes them onto the event bus. It also provides an
    endpoint to query for sensor configs via the event bus.
    """

    def __init__(self):
        super().__init__(topic="db_generic_sensors")
        self.__logger = logging.getLogger(__name__)

    async def __aenter__(self) -> Self:
        event_bus.register(self.topic + "/get_config", self.__get_sensor_config)
        event_bus.publish(self.topic + "/status_update", True)  # TODO: use a proper event
        return self

    async def __aexit__(
        self, exc_type: Type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        event_bus.unregister(self.topic + "/get_config")

    async def __get_sensor_config(self, uuid: UUID) -> dict[str, Any] | None:
        """
        Get all host configs from the database.

        Parameters
        ----------
        uuid: UUID
            The device uuid

        Returns
        -------
        dict
            A dictionary, that contains the configuration of the sensor.
        """
        try:
            device = await GenericSensorModel.find_one(GenericSensorModel.host == uuid)
        except (ValueError, pymongo.errors.ServerSelectionTimeoutError) as exc:
            # If the pydantic validation fails, we get a ValueError
            self.__logger.error("Invalid configuration for device %s. Ignoring configuration. Error: %s", uuid, exc)
            device = None

        if device is None:
            return device

        device = device.dict()
        # Rename the id key, because, the id is called uuid throughout the program. Note: This moves the uuid field to
        # the back of the dict
        device["uuid"] = device.pop("id")

        return device

    async def monitor_changes(self, timeout):
        """
        Push changes from the database onto the event_bus.

        Parameters
        ----------
        timeout: float
            The timeout in seconds to wait after a connection error.
        """
        async for change_type, change in self._monitor_database(GenericSensorModel, timeout):
            # Remember: Do not await in the iterator, as this stops the stream of updates
            if change_type == ChangeType.UPDATE:
                change_dict = change.dict()
                # Rename the id key, because we use the parameter uuid throughout the program, because `id` is already
                # used in Python
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['uuid']}/update", change_dict)
            elif change_type == ChangeType.ADD:
                change_dict = change.dict()
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['host']}/add", change_dict)
            elif change_type == ChangeType.REMOVE:
                # When removing sensors, the DB only returns the uuid
                event_bus.publish(f"nodes/by_uuid/{change_dict}/update", None)


class HostContext(Context):
    """
    The ethernet configuration database context manager. It monitors changes
    to the database and publishes them onto the event bus. It also provides an
    endpoint to query for sensor configs via the event bus.
    """

    def __init__(self):
        super().__init__(topic="db_autodiscovery_sensors")
        self.__logger = logging.getLogger(__name__)

    async def __aenter__(self) -> Self:
        event_bus.register(self.topic + "/get", self.__get_sensors)
        event_bus.register(self.topic + "/get_config", self.__get_sensor_config)
        event_bus.publish(self.topic + "/status_update", True)  # TODO: use a proper event
        return self

    async def __aexit__(
        self, exc_type: Type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        event_bus.unregister(self.topic + "/get")
        event_bus.unregister(self.topic + "/get_config")

    @staticmethod
    async def __get_sensors() -> AsyncGenerator[UUID, None]:
        """
        Get all gpib device ids from the database.

        Yields
        -------
        UUID
            The unique id of the device
        """
        # TODO: Handle database errors
        async for sensor in SensorHostModel.find_all(projection_model=BaseDocument):  # pylint: disable=not-an-iterable
            yield sensor.id

    async def __get_sensor_config(self, uuid: UUID) -> dict[str, Any] | None:
        """
        Get all host configs from the database.

        Parameters
        ----------
        uuid: UUID
            The device uuid

        Returns
        -------
        dict
            A dictionary, that contains the configuration of the sensor.
        """
        try:
            device = await SensorHostModel.find_one(SensorHostModel.id == uuid)
        except (ValueError, pymongo.errors.ServerSelectionTimeoutError) as exc:
            # If the pydantic validation fails, we get a ValueError
            self.__logger.error("Error while getting configuration for ethernet device %s: %s", uuid, exc)
            device = None

        if device is None:
            return device

        device = device.dict()
        # Rename the id key, because, the id is called uuid throughout the program. Note: This moves the uuid field to
        # the back of the dict
        device["uuid"] = device.pop("id")

        return device

    async def monitor_changes(self, timeout):
        """
        Push changes from the database onto the event_bus.

        Parameters
        ----------
        timeout: float
            The timeout in seconds to wait after a connection error.
        """
        change: SensorHostModel
        async for change_type, change in self._monitor_database(SensorHostModel, timeout):
            # Remember: Do not await in the iterator, as this stops the stream of updates
            if change_type == ChangeType.UPDATE:
                # Rename the id key, because we use the parameter uuid throughout the program, because `id` is already
                # used in Python
                change_dict = change.dict()
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['uuid']}/update", change_dict)
            elif change_type == ChangeType.ADD:
                change_dict = change.dict()
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"{self.topic}/add_host", change_dict["uuid"])

            elif change_type == ChangeType.REMOVE:
                # When removing sensors, the DB only returns the uuid
                event_bus.publish(f"nodes/by_uuid/{change}/update", None)


class TinkerforgeContext(Context):
    """
    The tinkerforge configuration database context manager. It monitors changes
    to the database and publishes them onto the event bus. It also provides an
    endpoint to query for sensor configs via the event bus.
    """

    def __init__(self):
        super().__init__(topic="db_tinkerforge_sensors")
        self.__logger = logging.getLogger(__name__)

    async def __aenter__(self) -> Self:
        event_bus.register(self.topic + "/get_config", self.__get_sensor_config)
        event_bus.publish(self.topic + "/status_update", True)  # TODO: use a proper event
        return self

    async def __aexit__(
        self, exc_type: Type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        event_bus.unregister(self.topic + "/get_config")

    async def __get_sensor_config(self, uid: int) -> dict[str, Any] | None:
        """
        Get all host configs from the database.

        Parameters
        ----------
        uid: int
            The Tinkerforge uid of the brick/bricklet

        Returns
        -------
        dict
            A dictionary, that contains the configuration of the sensor.
        """
        try:
            device = await TinkerforgeSensorModel.find_one(TinkerforgeSensorModel.uid == uid)
        except (ValueError, pymongo.errors.ServerSelectionTimeoutError) as exc:
            # If the pydantic validation fails, we get a ValueError
            self.__logger.error("Error while getting configuration for tinkerforge device %s: %s", uid, exc)
            device = None

        if device is None:
            return device

        device = device.dict()
        # Rename the id key, because, the id is called uuid throughout the program. Note: This moves the uuid field to
        # the back of the dict
        device["uuid"] = device.pop("id")

        return device

    async def monitor_changes(self, timeout):
        """
        Push changes from the database onto the event_bus.

        Parameters
        ----------
        timeout: float
            The timeout in seconds to wait after a connection error.
        """
        async for change_type, change in self._monitor_database(TinkerforgeSensorModel, timeout):
            # Remember: Do not await in the iterator, as this stops the stream of updates
            if change_type == ChangeType.UPDATE:
                change_dict = change.dict()
                # Rename the id key, because we use the parameter uuid throughout the program, because `id` is already
                # used in Python
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['uuid']}/remove", None)
                event_bus.publish(f"nodes/tinkerforge/{change_dict['uid']}/update", change_dict)
            elif change_type == ChangeType.ADD:
                change_dict = change.dict()
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['uuid']}/remove", None)
                event_bus.publish(f"nodes/tinkerforge/{change_dict['uid']}/update", change_dict)
            elif change_type == ChangeType.REMOVE:
                # When removing sensors, the DB only returns the uuid
                event_bus.publish(f"nodes/by_uuid/{change}/remove", None)


CONTEXTS = {
    GenericSensorContext,
    HostContext,
    LabnodeContext,
    TinkerforgeContext,
}
