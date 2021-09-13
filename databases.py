# -*- coding: utf-8 -*-
"""
The database abstraction layer, that handles all application specific tasks of
the databases.
"""

import asyncio
from enum import Enum, auto
import logging

from beanie import init_beanie
import motor.motor_asyncio
import pymongo  # to access the error classes
from pydantic.error_wrappers import ValidationError     # pylint: disable=no-name-in-module

from database.models import TinkerforgeSensor, GpibSensor, SensorHost


class ChangeType(Enum):
    """
    The type of changes sent out by the database.
    """
    ADD = auto()
    REMOVE = auto()
    UPDATE = auto()


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

    async def get_hosts(self):
        """
        Get all host configs from the database.

        Returns
        -------
        AsyncIterator[dict]
            An iterator that yields host configuration dictionaries.
        """
        try:
            # Get all hosts
            async for host in SensorHost.find_all():
                yield host.dict()
        except ValidationError:
            # TODO: Update the database in case there are old entries
            # TODO: Use SensorHost.inspect_collection to check the Schema
            self.__logger.error("Invalid host schema found. Cannot load database.")

    async def get_sensor_config(self, uid):
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
            return config.dict()
        return None

    async def monitor_host_changes(self, timeout):
        """
        Monitor all changes to the Tinkerforge host config table.

        Parameters
        ----------
        timeout: int
            The timeout in seconds to wait after a connection error.

        Returns
        -------
        AsyncIterator[tuple[ChangeType, dict]]
            An iterator that yields host configuration dictionaries and the
            type of change
        """
        resume_token = None
        # To watch only for certain events, use:
        # pipeline = [{'$match': {'operationType': {'$in': ["insert", "update", "delete"]}}}]
        pipeline = []
        while "loop not cancelled":
            try:
                async with SensorHost.get_motor_collection().watch(pipeline, full_document="updateLookup", resume_after=resume_token) as stream:
                    async for change in stream:
                        # TODO: catch parser errors!
                        if change['operationType'] == 'delete':
                            yield (ChangeType.REMOVE, change['documentKey']['_id'])
                        elif change['operationType'] == "update" or change['operationType'] == "replace":
                            yield (ChangeType.UPDATE, SensorHost.parse_obj(change['fullDocument']).dict())
                        elif change['operationType'] == "insert":
                            yield (ChangeType.ADD, SensorHost.parse_obj(change['fullDocument']).dict())

                    resume_token = stream.resume_token
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

    async def monitor_sensor_changes(self, timeout):
        """
        Monitor all changes to the Tinkerforge sensor config table.

        Parameters
        ----------
        AsyncIterator[tuple[ChangeType, dict]]
            An iterator that yields sensor configuration dictionaries and the
            type of change
        """
        resume_token = None
        while "loop not cancelled":
            try:
                async with TinkerforgeSensor.get_motor_collection().watch(full_document="updateLookup", resume_after=resume_token) as stream:
                    async for change in stream:
                        if change['operationType'] == 'delete':
                            yield (ChangeType.REMOVE, change['documentKey']['_id'])
                        elif change['operationType'] == "update" or change['operationType'] == "replace":
                            yield (ChangeType.UPDATE, TinkerforgeSensor.parse_obj(change['fullDocument']).dict())
                        elif change['operationType'] == "insert":
                            yield (ChangeType.ADD, TinkerforgeSensor.parse_obj(change['fullDocument']).dict())

                        resume_token = stream.resume_token
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                if resume_token is None:
                    # There is no usable resume token because there was a
                    # failure during ChangeStream initialization.
                    self.__logger.exception("Cannot resume, there is no token.")
                    await asyncio.sleep(timeout)
                else:
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
                self.__logger.info("Connecting to MongoDB on %s.", hostname_string)
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
                await init_beanie(database=database, document_models=[SensorHost, TinkerforgeSensor, GpibSensor])
                self.__logger.info("Connected to  MongoDB on %s.", hostname_string)
            except pymongo.errors.ServerSelectionTimeoutError as exc:
                if connection_attempt == 1:
                    # Only log the error once
                    self.__logger.error("Cannot connect to database on %s. Error: %s. Retrying.", hostname_string, exc)
                await asyncio.sleep(timeout)
                continue
            finally:
                connection_attempt += 1
            break
