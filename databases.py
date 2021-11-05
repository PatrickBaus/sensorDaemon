# -*- coding: utf-8 -*-
"""
The database abstraction layer, that handles all application specific tasks of
the databases.
"""

import asyncio
import logging

from beanie import init_beanie
import motor.motor_asyncio
import pymongo  # to access the error classes
from pydantic.error_wrappers import ValidationError     # pylint: disable=no-name-in-module

from data_types import ChangeType, UpdateChangeEvent, AddChangeEvent
from database.models import TinkerforgeSensor, GpibSensor, SensorHost, LabnodeSensor
from sensors.host_factory import host_factory


class MongoDb():
    """
    The Mongo DB abstractoin for the Tinkerforge settings database.
    """
    def __init__(self, hostname=None, port=None):
        self.__hostname = hostname
        self.__port = port
        self.__client = None
        self.__logger = logging.getLogger(__name__)

    async def __aenter__(self):
        await self.__connect()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        pass

    async def __connect(self):
        """
        Connect to the database. Retries if not successful.
        """
        connection_attempt = 1
        timeout = 0.5   # in s TODO: Make configurable
        while "database not connected":
            hostname_string = self.__hostname if self.__port is None else f"{self.__hostname}:{self.__port}"
            if connection_attempt == 1:
                self.__logger.info("Connecting to MongoDB (%s).", hostname_string)
            if self.__port is not None:
                self.__client = motor.motor_asyncio.AsyncIOMotorClient(
                    self.__hostname,
                    self.__port,
                    serverSelectionTimeoutMS=timeout*1000
                )
            else:
                self.__client = motor.motor_asyncio.AsyncIOMotorClient(
                    self.__hostname,
                    serverSelectionTimeoutMS=timeout*1000
                )

            database = self.__client.sensor_config
            try:
                await init_beanie(database=database, document_models=[SensorHost, TinkerforgeSensor, GpibSensor, LabnodeSensor])
                self.__logger.info("MongoDB (%s) connected.", hostname_string)
            except pymongo.errors.ServerSelectionTimeoutError as exc:
                if connection_attempt == 1:
                    # Only log the error once
                    self.__logger.error("Cannot connect to config database at %s. Error: %s. Retrying in %f s.", hostname_string, exc, timeout)
                await asyncio.sleep(timeout)
                continue
            finally:
                connection_attempt += 1
            break


class Context():    # pylint: disable=too-few-public-methods
    """
    The database context used by all config databases.
    """
    def __init__(self, event_bus):
        self._event_bus = event_bus
        self.__logger = logging.getLogger(__name__)

    async def _monitor_database(self, database_model, timeout):
        """
        Monitor all changes made to a certain database table.

        Parameters
        ----------
        database_model: beanie.Document
            The database model of the table to be monitored
        timeout: int
            The timeout in seconds to wait after a connection error.

        Returns
        -------
        AsyncIterator[tuple[ChangeType, dict]]
            An iterator that yields configuration dictionaries and the
            type of change
        """
        resume_token = None
        # To watch only for certain events, use:
        # pipeline = [{'$match': {'operationType': {'$in': ["insert", "update", "delete"]}}}]
        pipeline = []
        while "loop not cancelled":
            try:
                async with database_model.get_motor_collection().watch(pipeline, full_document="updateLookup", resume_after=resume_token) as change_stream:
                    async for change in change_stream:
                        # TODO: catch parser errors!
                        if change['operationType'] == 'delete':
                            yield (ChangeType.REMOVE, change['documentKey']['_id'])
                        elif change['operationType'] == "update" or change['operationType'] == "replace":
                            yield (ChangeType.UPDATE, database_model.parse_obj(change['fullDocument']))
                        elif change['operationType'] == "insert":
                            yield (ChangeType.ADD, database_model.parse_obj(change['fullDocument']))

                    resume_token = change_stream.resume_token
            except pymongo.errors.ServerSelectionTimeoutError as exc:
                self.__logger.error("Connection error while monitoring config database. Error: %s. Reconnecting in %f s.", exc, timeout)
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


class HostContext(Context):
    """
    The sensor host configuration database context manager. It monitors changes
    to the database and publishes them via the event bus.
    """
    def __init__(self, event_bus):
        super().__init__(event_bus)
        self.__logger = logging.getLogger(__name__)

    async def __aenter__(self):
        try:
            # Get all hosts
            async for host in SensorHost.find_all():    # A pylint bug. pylint: disable=not-an-iterable
                try:
                    host = host_factory.get(event_bus=self._event_bus, uid=host['id'], **host.dict())
                except ValueError:
                    # Ignore unknown drivers
                    logging.getLogger(__name__).info("Unsupported driver '%s' requested. Cannot start host.", host.dict()['driver'])
                else:
                    self._event_bus.publish("/hosts/add_host", AddChangeEvent(host))
        except ValidationError:
            # TODO: Update the database in case there are old entries
            # TODO: Use SensorHost.inspect_collection to check the Schema
            self.__logger.error("Invalid host schema found. Cannot load database.")
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        pass

    async def monitor_changes(self, timeout):
        """
        Monitor all changes to the sensor host config table.

        Parameters
        ----------
        timeout: int
            The timeout in seconds to wait after a connection error.

        Returns
        -------
        AsyncIterator[tuple[ChangeType, dict]]
            An iterator that yields configuration dictionaries and the
            type of change
        """
        async for change_type, change in self._monitor_database(SensorHost, timeout):
            if change_type is ChangeType.UPDATE:
                host = host_factory.get(event_bus=self._event_bus, **change.dict())
                self._event_bus.publish(f"/hosts/by_uuid/{change.id}/update", UpdateChangeEvent(host))
            elif change_type is ChangeType.ADD:
                host = host_factory.get(event_bus=self._event_bus, **change.dict())
                self._event_bus.publish("/hosts/add_host", AddChangeEvent(host))
            elif change_type is ChangeType.REMOVE:
                await self._event_bus.call(f"/hosts/by_uuid/{change}/disconnect", ignore_unregistered=True)


class TinkerforgeContext(Context):
    """
    The Tinkerforge configuration database context manager. It monitors changes
    to the database and publishes them onto the event bus. It also provides an
    endpoint to query for sensor configs via the event bus.
    """
    def __init__(self, event_bus):
        super().__init__(event_bus)
        self.__sensors = {}

    async def __aenter__(self):
        self._event_bus.register("/database/tinkerforge/get_sensor_config", self.__get_sensor_config)
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self._event_bus.unregister("/database/tinkerforge")

    async def __get_sensor_config(self, uid):
        """
        Get all host configs from the database.

        Parameters
        ----------
        uid: int
            The unique id of the Tinkerforge sensor

        Returns
        -------
        dict
            A dictionary, that contains the configuration of the sensor.
        """
        config = await TinkerforgeSensor.find_one(TinkerforgeSensor.uid == uid)
        if config is not None:
            # Keep a copy of the uuid -> uid, so that we can later remove the config,
            # because the DB will only send the uuid on removal.
            self.__sensors[config.id] = config.uid
            return config.dict()
        return {}

    async def monitor_changes(self, timeout):
        """
        Adds the driver string to the output of the iterator. We do not remove
        any sensors, if the config is removed, because only the tinkerforge host
        is allowed to remove sensors. We unconfigure them instead.
        """
        async for change_type, change in self._monitor_database(TinkerforgeSensor, timeout):
            if change_type == ChangeType.ADD:
                self.__sensors[change.id] = change.uid
                self._event_bus.publish(f"/sensors/tinkerforge/by_uid/{change.uid}/update", UpdateChangeEvent(change.dict()))
            elif change_type == ChangeType.UPDATE:
                sensor_uid = self.__sensors.pop(change.id, None)
                if sensor_uid is not None and sensor_uid != change.uid:
                    # The uid was changed, so we need to unregister the old sensor
                    self._event_bus.publish(f"/sensors/tinkerforge/by_uid/{sensor_uid}/update", UpdateChangeEvent({}))
                self.__sensors[change.id] = change.uid
                self._event_bus.publish(f"/sensors/tinkerforge/by_uid/{change.uid}/update", UpdateChangeEvent(change.dict()))
            elif change_type == ChangeType.REMOVE:
                # When removing sensors, the DB only returns the uuid
                sensor_uid = self.__sensors.pop(change, None)
                if sensor_uid:
                    self._event_bus.publish(f"/sensors/tinkerforge/by_uid/{sensor_uid}/update", UpdateChangeEvent({}))


class PrologixGpibContext(Context):
    """
    The Prologix GPIB configuration database context manager. It monitors changes
    to the database and publishes them onto the event bus. It also provides an
    endpoint to query for sensor configs via the event bus.
    """
    async def __aenter__(self):
        self._event_bus.register("/database/gpib/get_sensors", self.__get_sensors)
        self._event_bus.register("/database/gpib/get_sensor_config", self.__get_sensor_config)
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self._event_bus.unregister("/database/gpib/get_sensors")
        self._event_bus.unregister("/database/gpib/get_sensor_config")

    @staticmethod
    async def __get_sensors(host_uuid):
        """
        Get all host configs from the database.

        Parameters
        ----------
        host_uuid: int
            The unique id of the Tinkerforge sensor

        Returns
        -------
        dict
            A dictionary, that contains the configuration of the sensor.
        """
        async for config in GpibSensor.find(GpibSensor.host == host_uuid):  # A pylint bug. pylint: disable=not-an-iterable
            if config is not None:
                yield config.dict()

    @staticmethod
    async def __get_sensor_config(uuid):
        """
        Get all host configs from the database.

        Parameters
        ----------
        uid: int
            The unique id of the Tinkerforge sensor

        Returns
        -------
        dict
            A dictionary, that contains the configuration of the sensor.
        """
        return (await GpibSensor.find_one(GpibSensor.id == uuid)).dict()

    async def monitor_changes(self, timeout):
        """
        Adds the driver string to the output of the iterator.
        """
        async for change_type, change in self._monitor_database(GpibSensor, timeout):
            if change_type == ChangeType.ADD:
                self._event_bus.publish(f"/hosts/by_uuid/{change.host}/add_sensor", AddChangeEvent(change.dict()))
            elif change_type == ChangeType.UPDATE:
                self._event_bus.publish(f"/sensors/gpib/by_uid/{change.id}/update", UpdateChangeEvent(change.dict()))
            elif change_type == ChangeType.REMOVE:
                await self._event_bus.call(f"/sensors/gpib/by_uid/{change}/disconnect", ignore_unregistered=True)


class LabnodeContext(Context):
    """
    The Labnode configuration database context manager. It monitors changes
    to the database and publishes them onto the event bus. It also provides an
    endpoint to query for sensor configs via the event bus.
    """
    def __init__(self, event_bus):
        super().__init__(event_bus)
        self.__sensors = {}

    async def __aenter__(self):
        self._event_bus.register("/database/labnode/get_sensor_config", self.__get_sensor_config)
        self.__sensors = {}
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self._event_bus.unregister("/database/labnode/get_sensor_config")

    async def __get_sensor_config(self, uid):
        """
        Get all host configs from the database.

        Parameters
        ----------
        uid: int
            The serial number of the labnode sensor

        Returns
        -------
        dict
            A dictionary, that contains the configuration of the sensor.
        """
        config = await LabnodeSensor.find_one(LabnodeSensor.uid == uid)
        if config is not None:
            self.__sensors[config.id] = config.uid
            return config.dict()
        return {}

    async def monitor_changes(self, timeout):
        """
        Adds the driver string to the output of the iterator.
        """
        async for change_type, change in self._monitor_database(LabnodeSensor, timeout):
            if change_type == ChangeType.ADD:
                self.__sensors[change.id] = change.uid
                self._event_bus.publish(f"/sensors/labnode/by_uid/{change.uid}/update", UpdateChangeEvent(change.dict()))
            elif change_type == ChangeType.UPDATE:
                sensor_uid = self.__sensors.pop(change.id, None)
                if sensor_uid is not None and sensor_uid != change.uid:
                    # The uid was changed, so we need to unregister the old sensor
                    self._event_bus.publish(f"/sensors/labnode/by_uid/{sensor_uid}/update", UpdateChangeEvent({}))
                self.__sensors[change.id] = change.uid
                self._event_bus.publish(f"/sensors/labnode/by_uid/{change.uid}/update", UpdateChangeEvent(change.dict()))
            elif change_type == ChangeType.REMOVE:
                # When removing sensors, the DB only returns the uuid
                sensor_uid = self.__sensors.pop(change, None)
                if sensor_uid is not None:
                    self._event_bus.publish(f"/sensors/labnode/by_uid/{sensor_uid}/update", UpdateChangeEvent({}))


CONTEXTS = {
    HostContext,
    TinkerforgeContext,
    PrologixGpibContext,
    LabnodeContext,
}
