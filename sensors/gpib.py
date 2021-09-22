# -*- coding: utf-8 -*-
"""
This file contains the wrapper for the Tinkerforge sensor class
"""
import asyncio
import logging
import time

from aiostream import stream, pipe
from prologix_gpib_async import AsyncPrologixGpibEthernetController

from data_types import UpdateChangeEvent, RemoveChangeEvent
from .sensor_factory import gpib_device_factory

# Event bus topics
EVENT_BUS_BASE = "/sensors/gpib"
EVENT_BUS_CONFIG_UPDATE_BY_UID = EVENT_BUS_BASE + "/by_uid/{uid}/update"
EVENT_BUS_DISCONNECT_BY_UID = EVENT_BUS_BASE + "/by_uid/{uid}/disconnect"
EVENT_BUS_STATUS = EVENT_BUS_BASE

MAXIMUM_FAILED_CONNECTIONS = 3


class PrologixGpibSensor():
    """
    The wrapper class for all Tinkerforge sensors. It combines the database config
    with the underlying hardware.
    """
    @property
    def uuid(self):
        """
        Returns
        -------
        int
            The sensor hardware uid.
        """
        return self.__uuid

    def __init__(self, hostname, port, pad, sad, uuid, reconnect_interval):
        self.__conn = AsyncPrologixGpibEthernetController(
            hostname=hostname,
            port=port,
            pad=pad,
            sad=sad
        )
        self.__uuid = uuid
        self.__gpib_device = None
        self.__logger = logging.getLogger(__name__)
        self.__reconnect_interval = reconnect_interval

    def __str__(self):
        return str(self.__conn)

    def __repr__(self):
        return f"{self.__class__.__module__}.{self.__class__.__qualname__}(hostname={self.__conn.hostname}, port={self.__conn.port}, pad={self.__conn.pad}, sad={self.__conn.sad}) uid={self.uuid}"

    async def __aenter__(self):
        failed_connection_attemps = 0
        while "not connected":
            try:
                await self.__conn.connect()
                return self
            except ConnectionError as exc:
                failed_connection_attemps += 1
                # Suppress the warning after MAXIMUM_FAILED_CONNECTIONS to stop spamming log files
                if failed_connection_attemps < MAXIMUM_FAILED_CONNECTIONS:
                    if failed_connection_attemps > 1:
                        failure_count = " (%d times)" % failed_connection_attemps
                    else:
                        failure_count = ""
                    self.__logger.warning("Failed to connect to host '%s:%i'%s. Error: %s.", self.__conn.hostname, self.__conn.port, failure_count, exc)
                if failed_connection_attemps == MAXIMUM_FAILED_CONNECTIONS:
                    self.__logger.warning("Failed to connect to host '%s:%i' (%d time%s). Error: %s. Suppressing warnings from hereon.", self.__conn.hostname, self.__conn.port, failed_connection_attemps, "s"[failed_connection_attemps == 1:], exc)
                await asyncio.sleep(self.__reconnect_interval)

    async def __aexit__(self, exc_type, exc, traceback):
        await self.__conn.disconnect()

    async def __configure(self, config):
        if config['interval'] == 0:
            return 0
        self.__gpib_device = gpib_device_factory.get(config['driver'], self.__conn)
        for cmd in config['on_connect']:
            try:
                function = getattr(self.__gpib_device, cmd["function"])
            except AttributeError:
                self.__logger.error("Invalid configuration parameter '%s' for sensor %s", cmd["function"], self.__gpib_device)
                continue
            try:
                result = function(*cmd.get("args", []), **cmd.get("kwargs", {}))
                if asyncio.iscoroutine(result):
                    await result
            except Exception:   # pylint: disable=broad-except
                # Catch all exceptions and log them, because this is an external input
                self.__logger.exception("Error processing config for sensor %s on host '%s'", self.uuid, self.__conn.hostname)
                continue
        return config['interval']/1000

    @staticmethod
    def is_no_event(event_type):
        def filter_event(item):
            return not isinstance(item, event_type)

        return filter_event

    async def __configure_and_read(self, event_bus, config, old_config=None):
        interval = await self.__configure(config)
        if interval > 0:
            merged_stream = (
                stream.merge(
                    stream.iterate(self.__gpib_device.read_all()) | pipe.spaceout(interval),  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
                    stream.iterate(event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE_BY_UID.format(uid=self.uuid)))
                )
                | pipe.takewhile(self.is_no_event(UpdateChangeEvent))  # Terminate if the config was updated https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            )
            async with merged_stream.stream() as streamer:
                async for item in streamer:
                    yield item

    async def read_events(self, event_bus, ping_interval):
        """
        Read the sensor data, and ping the sensor.

        Returns
        -------
        Iterator[dict]
            The sensor data.
        """
        assert ping_interval > 0
        config = await event_bus.call("/database/gpib/get_sensor_config", self.uuid)
        new_streams_queue = asyncio.Queue()
        new_streams_queue.put_nowait(self.__configure_and_read(event_bus, config))
        new_streams_queue.put_nowait(event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE_BY_UID.format(uid=self.uuid)))
        new_streams_queue.put_nowait(event_bus.subscribe(EVENT_BUS_DISCONNECT_BY_UID.format(uid=self.uuid)))

        def filter_updates(item):
            if isinstance(item, UpdateChangeEvent):
                # Reconfigure the device, then start reading from it.
                # The __configure_and_read method, will automatically shut
                # down.
                nonlocal config, new_streams_queue
                old_config = config
                config = item.change
                new_streams_queue.put_nowait(self.__configure_and_read(event_bus, config, old_config))
                return False
            return True

        merged_stream = (
            stream.call(new_streams_queue.get)
            | pipe.cycle()  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            | pipe.flatten()  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            | pipe.takewhile(self.is_no_event(RemoveChangeEvent))  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            | pipe.filter(filter_updates)  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
        )
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                yield {
                    'timestamp': time.time(),
                    'sender': self,
                    'sid': config['sad'],
                    'payload': item,
                    'topic': config['topic']
                }
