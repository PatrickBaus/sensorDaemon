"""
The database abstraction layer, that handles all application specific tasks of
the databases.
"""
from __future__ import annotations

import asyncio
import logging
import re
from types import TracebackType
from typing import Any, AsyncGenerator, NotRequired, Type, TypedDict, Unpack

try:
    from typing import Self  # type: ignore # Python 3.11
except ImportError:
    from typing_extensions import Self

from uuid import UUID

import motor.motor_asyncio
import pymongo  # to access the error classes
from beanie import Document, init_beanie

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


class DatabaseParams(TypedDict):
    """Parameters used for the configuration database. These will be passed to MongoClient()."""

    host: str
    username: NotRequired[str | None]
    password: NotRequired[str | None]


class MongoDb:
    """
    The Mongo DB abstraction for the settings database.
    """

    def __init__(self, **kwargs: Unpack[DatabaseParams]) -> None:
        self.__params = kwargs
        self.__client = None
        self.__logger = logging.getLogger(__name__)

    @property
    def hostname(self) -> str:
        """Return the hostname including the port. Strips usernames and passwords."""
        # strip usernames and password
        return self.__params["host"][self.__params["host"].find("@") + 1 : self.__params["host"].rfind("/?")]

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
            if connection_attempt == 1:
                self.__logger.info(
                    "Connecting to MongoDB (%s).",
                    self.hostname,
                )
            self.__client = motor.motor_asyncio.AsyncIOMotorClient(
                **self.__params, serverSelectionTimeoutMS=timeout * 1000, uuidRepresentation="standard"
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
                self.__logger.info(
                    "MongoDB (%s) connected.",
                    self.hostname,
                )
            except pymongo.errors.AutoReconnect as exc:
                if connection_attempt == 1:
                    # Only log the error once
                    self.__logger.error(
                        "Cannot connect to config database at %s. Error: %s. Retrying in %f s.",
                        self.hostname,
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

    def _log_database_error(
        self,
        database_model: Type[DeviceDocument],
        error_code: str | int | None,
        previous_error_code: str | int | None,
        timeout: float,
    ):
        if error_code == previous_error_code:
            return  # Only log an error once

        database_name = None if database_model.get_settings() is None else database_model.get_settings().name
        database_name = str(database_model) if database_name is None else database_name

        if error_code is None:
            self.__logger.info("Database worker (%s): Database connected.", database_name)
        elif error_code == -2:
            self.__logger.error("Database worker (%s): Failure in name resolution. Retrying.", database_name)
        elif error_code == 104:
            self.__logger.error("Database worker (%s): Database disconnected. Waiting to restart.", database_name)
        elif error_code == 111:
            self.__logger.error("Database worker (%s): Connection refused. Waiting to start.", database_name)
        elif error_code == 211:
            self.__logger.error("Database worker (%s): Initialising replica set. Waiting to start.", database_name)
        elif error_code == "resume_token":
            self.__logger.error(
                "Database worker (%s): Cannot resume Mongo DB change stream, there is no token. Starting from scratch.",
                database_name,
            )
        else:
            self.__logger.error(
                "Database worker (%s): Connection error while monitoring database. Error: %s. Reconnecting in %.2f s.",
                database_name,
                error_code,
                timeout,
            )

    async def _monitor_database(
        self, database_model: Type[DeviceDocument], timeout: float
    ) -> AsyncGenerator[tuple[ChangeType, UUID | Document], None]:
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
        tuple of ChangeType and UUID or Document
            An iterator that yields the type of change and either the new DeviceDocument or the UUID of the deleted
            device
        """
        resume_token = None
        previous_error_code: str | int | None = None
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
                    previous_error_code = None
                    self._log_database_error(database_model, previous_error_code, 0, timeout=0)
                    async for change in change_stream:
                        # TODO: catch parser errors!
                        if change["operationType"] == "delete":
                            yield ChangeType.REMOVE, change["documentKey"]["_id"]
                        elif change["operationType"] == "update" or change["operationType"] == "replace":
                            yield ChangeType.UPDATE, database_model.model_validate(change["fullDocument"])
                        elif change["operationType"] == "insert":
                            yield ChangeType.ADD, database_model.model_validate(change["fullDocument"])

                    resume_token = change_stream.resume_token
            except pymongo.errors.AutoReconnect as exc:
                error = re.search(r"\[Errno ([+-]?\d+)\]", str(exc))
                error_code: str | int = str(exc) if error is None else int(error.group(1))
                self._log_database_error(database_model, error_code, previous_error_code, timeout)
                previous_error_code = error_code
                await asyncio.sleep(timeout)
            except pymongo.errors.OperationFailure as exc:
                error_code = exc.code if exc.code == 211 else str(exc)
                self._log_database_error(database_model, error_code, previous_error_code, timeout)
                previous_error_code = error_code
                await asyncio.sleep(timeout)
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                if resume_token is None:
                    # There is no usable resume token because there was a
                    # failure during ChangeStream initialization.
                    error_code = "resume_token"
                    self._log_database_error(database_model, error_code, previous_error_code, timeout)
                    previous_error_code = error_code

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
        connection_attempt = 0
        while "not connected":
            connection_attempt += 1
            try:
                device = await LabnodeSensorModel.find_one(LabnodeSensorModel.id == uuid)
            except ValueError as exc:
                # If the pydantic validation fails, we get a ValueError
                self.__logger.error("Error while getting configuration for LabNode device %s: %s", uuid, exc)
                device = None
            except pymongo.errors.AutoReconnect:
                # TODO: Do not query again. Wait for a message from the database
                if connection_attempt == 1:
                    self.__logger.info(
                        "Waiting for LabNode database to come back online. Query for device %s on hold.", uuid
                    )
                await asyncio.sleep(0.5)
                continue
            except pymongo.errors.OperationFailure as exc:
                if exc.code == 211:  # Cache Reader No keys found for HMAC that is valid for time
                    if connection_attempt == 1:
                        self.__logger.info(
                            "Waiting for LabNode database to come back online. Query for device %s on hold.", uuid
                        )
                    await asyncio.sleep(0.5)
                    continue
                self.__logger.exception("Error while querying LabNode database.")
                device = None
            except pymongo.errors.PyMongoError:
                self.__logger.exception("Error while querying LabNode database.")
                device = None

            if device is None:
                return device

            device = device.dict()
            # Rename the id key, because, the id is called uuid throughout the program. Note: This moves the uuid field
            # to the back of the dict
            device["uuid"] = device.pop("id")

            return device
        assert False, "unreachable"

    async def monitor_changes(self, timeout: float) -> None:
        """
        Push changes from the database onto the event_bus.

        Parameters
        ----------
        timeout: float
            The timeout in seconds to wait after a connection error.
        """
        async for change_type, change in self._monitor_database(LabnodeSensorModel, timeout):
            # Remember: Do not await in the iterator, as this stops the stream of updates
            if change_type == ChangeType.UPDATE:
                assert isinstance(change, LabnodeSensorModel)
                change_dict = change.dict()
                # Rename the id key, because we use the parameter uuid throughout the program, because `id` is already
                # used in Python
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['uuid']}/update", change_dict)
            elif change_type == ChangeType.ADD:
                assert isinstance(change, LabnodeSensorModel)
                change_dict = change.dict()
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['uuid']}/add", change_dict)
            elif change_type == ChangeType.REMOVE:
                # When removing sensors, the DB only returns the uuid
                assert isinstance(change, UUID)
                event_bus.publish(f"nodes/by_uuid/{change}/update", None)


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
        connection_attempt = 0
        while "not connected":
            connection_attempt += 1
            try:
                device = await GenericSensorModel.find_one(GenericSensorModel.host == uuid)
            except ValueError as exc:
                # If the pydantic validation fails, we get a ValueError
                self.__logger.error(
                    "Invalid configuration for generic device %s. Ignoring configuration. Error: %s", uuid, exc
                )
                device = None
            except pymongo.errors.AutoReconnect:
                # TODO: Do not query again. Wait for a message from the database
                if connection_attempt == 1:
                    self.__logger.info(
                        "Waiting for generic device database to come back online. Query for host %s on hold.", uuid
                    )
                await asyncio.sleep(0.5)
                continue
            except pymongo.errors.OperationFailure as exc:
                if exc.code == 211:  # Cache Reader No keys found for HMAC that is valid for time
                    if connection_attempt == 1:
                        self.__logger.info(
                            "Waiting for generic device database to come back online. Query for device %s on hold.",
                            uuid,
                        )
                    await asyncio.sleep(0.5)
                    continue
                self.__logger.exception("Error while querying generic device database.")
                device = None
            except pymongo.errors.PyMongoError:
                self.__logger.exception("Error while querying generic device database.")
                device = None

            if device is None:
                return device

            device = device.dict()
            # Rename the id key, because, the id is called uuid throughout the program. Note: This moves the uuid field
            # to the back of the dict
            device["uuid"] = device.pop("id")

            return device
        assert False, "unreachable"

    async def monitor_changes(self, timeout: float) -> None:
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
                assert isinstance(change, GenericSensorModel)
                change_dict = change.dict()
                # Rename the id key, because we use the parameter uuid throughout the program, because `id` is already
                # used in Python
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['uuid']}/update", change_dict)
            elif change_type == ChangeType.ADD:
                assert isinstance(change, GenericSensorModel)
                change_dict = change.dict()
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['host']}/add", change_dict)
            elif change_type == ChangeType.REMOVE:
                assert isinstance(change, UUID)
                # When removing sensors, the DB only returns the uuid
                event_bus.publish(f"nodes/by_uuid/{change}/update", None)


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
        connection_attempt = 0
        while "not connected":
            connection_attempt += 1
            try:
                device = await SensorHostModel.find_one(SensorHostModel.id == uuid)
            except ValueError as exc:
                # If the pydantic validation fails, we get a ValueError
                self.__logger.error("Error while getting configuration for host %s: %s", uuid, exc)
                device = None
            except pymongo.errors.AutoReconnect:
                # TODO: Do not query again. Wait for a message from the database
                if connection_attempt == 1:
                    self.__logger.info(
                        "Waiting for the host database to come back online. Query for host %s on hold.", uuid
                    )
                await asyncio.sleep(0.5)
                continue
            except pymongo.errors.OperationFailure as exc:
                if exc.code == 211:  # Cache Reader No keys found for HMAC that is valid for time
                    if connection_attempt == 1:
                        self.__logger.info(
                            "Waiting for host database to come back online. Query for host %s on hold.", uuid
                        )
                    await asyncio.sleep(0.5)
                    continue
                self.__logger.exception("Error while querying host database.")
                device = None
            except pymongo.errors.PyMongoError:
                self.__logger.exception("Error while querying host database.")
                device = None

            if device is None:
                return device

            device = device.dict()
            # Rename the id key, because, the id is called uuid throughout the program. Note: This moves the uuid field
            # to the back of the dict
            device["uuid"] = device.pop("id")

            return device
        assert False, "unreachable"

    async def monitor_changes(self, timeout: float) -> None:
        """
        Push changes from the database onto the event_bus.

        Parameters
        ----------
        timeout: float
            The timeout in seconds to wait after a connection error.
        """
        async for change_type, change in self._monitor_database(SensorHostModel, timeout):
            # Remember: Do not await in the iterator, as this stops the stream of updates
            if change_type == ChangeType.UPDATE:
                # Rename the id key, because we use the parameter uuid throughout the program, because `id` is already
                # used in Python
                assert isinstance(change, SensorHostModel)
                change_dict = change.dict()
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['uuid']}/update", change_dict)
            elif change_type == ChangeType.ADD:
                assert isinstance(change, SensorHostModel)
                change_dict = change.dict()
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"{self.topic}/add_host", change_dict["uuid"])
            elif change_type == ChangeType.REMOVE:
                # When removing sensors, the DB only returns the uuid
                assert isinstance(change, UUID)
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
        connection_attempt = 0
        while "not connected":
            connection_attempt += 1
            try:
                device = await TinkerforgeSensorModel.find_one(TinkerforgeSensorModel.uid == uid)
            except ValueError as exc:
                # TODO: Handle the timeout and retry getting the config, otherwise a sensor might end up unconfigured...
                # If the pydantic validation fails, we get a ValueError
                self.__logger.error("Error while getting configuration for Tinkerforge device %s: %s", uid, exc)
                device = None
            except pymongo.errors.AutoReconnect:
                # TODO: Do not query again. Wait for a message from the database
                if connection_attempt == 1:
                    self.__logger.info(
                        "Waiting for Tinkerforge database to come back online. Query for device %s on hold.", uid
                    )
                await asyncio.sleep(0.5)
                continue
            except pymongo.errors.OperationFailure as exc:
                if exc.code == 211:  # Cache Reader No keys found for HMAC that is valid for time
                    if connection_attempt == 1:
                        self.__logger.info(
                            "Waiting for Tinkerforge database to come back online. Query for device %s on hold.", uid
                        )
                    await asyncio.sleep(0.5)
                    continue
                self.__logger.exception("Error while querying Tinkerforge database.")
                device = None
            except pymongo.errors.PyMongoError:
                self.__logger.exception("Error while querying Tinkerforge database.")
                device = None

            if device is None:
                return device

            device = device.dict()
            # Rename the id key, because, the id is called uuid throughout the program. Note: This moves the uuid field
            # to the back of the dict
            device["uuid"] = device.pop("id")

            return device
        assert False, "unreachable"

    async def monitor_changes(self, timeout: float) -> None:
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
                assert isinstance(change, TinkerforgeSensorModel)
                change_dict = change.dict()
                # Rename the id key, because we use the parameter uuid throughout the program, because `id` is already
                # used in Python
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['uuid']}/remove", None)
                event_bus.publish(f"nodes/tinkerforge/{change_dict['uid']}/update", change_dict)
            elif change_type == ChangeType.ADD:
                assert isinstance(change, TinkerforgeSensorModel)
                change_dict = change.dict()
                change_dict["uuid"] = change_dict.pop("id")  # Note uuid will be moved to the end of the dict.
                event_bus.publish(f"nodes/by_uuid/{change_dict['uuid']}/remove", None)
                event_bus.publish(f"nodes/tinkerforge/{change_dict['uid']}/update", change_dict)
            elif change_type == ChangeType.REMOVE:
                # When removing sensors, the DB only returns the uuid
                assert isinstance(change, UUID)
                event_bus.publish(f"nodes/by_uuid/{change}/remove", None)


CONTEXTS = {
    GenericSensorContext,
    HostContext,
    LabnodeContext,
    TinkerforgeContext,
}
