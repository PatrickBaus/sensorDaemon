# -*- coding: utf-8 -*-
"""
This file contains the wrapper for the Tinkerforge sensor class
"""
import asyncio
from inspect import isasyncgen, iscoroutine
import logging
import time

from aiostream import stream, pipe
from prologix_gpib_async import AsyncPrologixGpibEthernetController, EosMode

from data_types import UpdateChangeEvent
from errors import DisconnectedDuringConnectError
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
    def hostname(self):
        """
        Returns
        -------
        str
            The host name of the ip connection.
        """
        return self.__conn.hostname

    @property
    def port(self):
        """
        Returns
        -------
        int
            The port of the ip connection.
        """
        return self.__conn.port

    @property
    def pad(self):
        """
        Returns
        -------
        int
            The primary address of the device.
        """
        return self.__conn.pad

    @property
    def sad(self):
        """
        Returns
        -------
        int
            The secondary address of the device.
        """
        return self.__conn.sad

    @property
    def reconnect_interval(self):
        """
        The reconnect is only attempted during initial connection.

        Returns
        -------
        int
            The reconnect interval in seconds.
        """
        return self.__reconnect_interval

    @property
    def uuid(self):
        """
        Returns
        -------
        int
            The sensor hardware uid.
        """
        return self.__uuid

    def __init__(self, hostname, port, pad, sad, uuid, event_bus, reconnect_interval):
        self.__conn = AsyncPrologixGpibEthernetController(
            hostname=hostname,
            port=port,
            pad=pad,
            sad=sad,
            eos_mode=EosMode.APPEND_NONE,
        )
        self.__uuid = uuid
        self.__gpib_device = None
        self.__logger = logging.getLogger(__name__)
        self.__event_bus = event_bus
        self.__reconnect_interval = reconnect_interval
        self.__shutdown_event = asyncio.Event()
        self.__shutdown_event.set()   # Force the use of __aenter__()

    def __str__(self):
        return str(self.__conn)

    def __repr__(self):
        return f"{self.__class__.__module__}.{self.__class__.__qualname__}(hostname={self.hostname}, port={self.port}, pad={self.pad}, sad={self.sad}) uid={self.uuid}"

    async def __aenter__(self):
        failed_connection_attemps = 0
        self.__shutdown_event.clear()
        self.__event_bus.register(EVENT_BUS_DISCONNECT_BY_UID.format(uid=self.uuid), self.__disconnect)
        while "not connected":
            try:
                pending = set()
                is_disconnected_task = asyncio.create_task(self.__shutdown_event.wait())
                pending.add(is_disconnected_task)
                connect_task = asyncio.create_task(self.__conn.connect())
                pending.add(connect_task)
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task == is_disconnected_task:    # pylint: disable=no-else-raise
                        await self.__conn.disconnect()
                        raise DisconnectedDuringConnectError()
                    else:
                        is_disconnected_task.cancel()
                        task.result()   # If there is an error, blow up now
                        return self
            except ConnectionError as exc:
                failed_connection_attemps += 1
                # Suppress the warning after MAXIMUM_FAILED_CONNECTIONS to stop spamming log files
                if failed_connection_attemps < MAXIMUM_FAILED_CONNECTIONS:
                    if failed_connection_attemps > 1:
                        failure_count = " (%d times)" % failed_connection_attemps
                    else:
                        failure_count = ""
                    self.__logger.warning("Failed to connect to host '%s:%i'%s. Error: %s.", self.hostname, self.port, failure_count, exc)
                if failed_connection_attemps == MAXIMUM_FAILED_CONNECTIONS:
                    self.__logger.warning("Failed to connect to host '%s:%i' (%d time%s). Error: %s. Suppressing warnings from hereon.", self.hostname, self.port, failed_connection_attemps, "s"[failed_connection_attemps == 1:], exc)
                await asyncio.sleep(self.__reconnect_interval)

    async def __aexit__(self, exc_type, exc, traceback):
        self.__event_bus.unregister(EVENT_BUS_DISCONNECT_BY_UID.format(uid=self.uuid))
        self.__shutdown_event.set()
        await self.__conn.disconnect()

    async def __disconnect(self):
        self.__shutdown_event.set()

    async def __configure(self, config):
        on_before_read, on_after_read = [], []
        self.__gpib_device = gpib_device_factory.get(config['driver'], self.__conn)

        function = getattr(self.__gpib_device, config["on_read"]["function"])
        on_read = (function, config["on_read"].get("args", []), config["on_read"].get("kwargs", {}))

        if config['interval'] == 0:
            # Return here, if the device is disabled
            return (0, on_read, on_before_read, on_after_read)

        # Initialize the device
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
                self.__logger.exception("Error processing config for sensor %s at host '%s:%i'", self.uuid, self.hostname, self.port)
                continue

        for cmd in config['on_before_read']:
            try:
                function = getattr(self.__gpib_device, cmd["function"])
            except AttributeError:
                self.__logger.error("Invalid configuration parameter '%s' for sensor %s", cmd["function"], self.__gpib_device)
            else:
                on_before_read.append((function, cmd.get("args", []), cmd.get("kwargs", {})))

        for cmd in config['on_after_read']:
            try:
                function = getattr(self.__gpib_device, cmd["function"])
            except AttributeError:
                self.__logger.error("Invalid configuration parameter '%s' for sensor %s", cmd["function"], self.__gpib_device)
            else:
                on_after_read.append((function, cmd.get("args", []), cmd.get("kwargs", {})))

        return config['interval']/1000, on_read, on_before_read, on_after_read

    @staticmethod
    def is_no_event(event_type):
        def filter_event(item):
            return not isinstance(item, event_type)

        return filter_event

    async def __call_functions_on_device(self, func_calls):
        coros = []
        for func_call in func_calls:
            func, args, kwargs = func_call
            try:
                if iscoroutine(func):
                    coros.append(func(*args, **kwargs))
                else:
                    func(*args, **kwargs)
            except Exception:
                self.__logger.exception("Invalid configuration parameter '%s' for sensor %s", func, self.__gpib_device)
                raise
        if coros:
            results = await asyncio.gather(*coros, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    self.__logger.error("Error while reading results from sensor %s", self.__gpib_device, exc_info=result)
                    raise result

    async def __read_device(self, on_read, on_before_read, on_after_read):
        func, args, kwargs = on_read
        gen_or_func = func(*args, **kwargs)
        if isasyncgen(gen_or_func):
            await self.__call_functions_on_device(on_before_read)
            async for result in gen_or_func:
                yield result
                await self.__call_functions_on_device(on_after_read)
        else:
            while "loop not cancelled":
                await self.__call_functions_on_device(on_before_read)
                try:
                    if iscoroutine(gen_or_func):
                        yield await gen_or_func
                    else:
                        yield gen_or_func
                except Exception:
                    self.__logger.exception("Error while reading results from sensor %s", self.__gpib_device)
                    raise
                await self.__call_functions_on_device(on_after_read)

    async def __configure_and_read(self, config, old_config=None):
        interval, on_read, on_before_read, on_after_read = await self.__configure(config)
        if interval > 0:
            merged_stream = (
                stream.merge(
                    stream.iterate(self.__read_device(on_read, on_before_read, on_after_read)) | pipe.spaceout(interval),  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
                    stream.iterate(self.__event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE_BY_UID.format(uid=self.uuid)))
                )
                | pipe.takewhile(self.is_no_event(UpdateChangeEvent))  # Terminate if the config was updated https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            )
            async with merged_stream.stream() as streamer:
                async for item in streamer:
                    yield item

    async def read_events(self, ping_interval):
        """
        Read the sensor data, and ping the sensor.

        Returns
        -------
        Iterator[dict]
            The sensor data.
        """
        assert ping_interval > 0
        config = await self.__event_bus.call("/database/gpib/get_sensor_config", self.uuid)
        new_streams_queue = asyncio.Queue()
        new_streams_queue.put_nowait(self.__configure_and_read(config))
        new_streams_queue.put_nowait(self.__event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE_BY_UID.format(uid=self.uuid)))
        new_streams_queue.put_nowait(stream.just(self.__shutdown_event.wait()))

        def filter_updates(item):
            if isinstance(item, UpdateChangeEvent):
                # Reconfigure the device, then start reading from it.
                # The __configure_and_read method, will automatically shut
                # down.
                nonlocal config, new_streams_queue
                old_config = config
                config = item.change
                new_streams_queue.put_nowait(self.__configure_and_read(config, old_config))
                return False    # Do not pass on the update, because we have already processed it
            return True

        merged_stream = (
            stream.call(new_streams_queue.get)
            | pipe.cycle()  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            | pipe.flatten()  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            | pipe.filter(filter_updates)  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
        )
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                if self.__shutdown_event.is_set():
                    break
                yield {
                    'timestamp': time.time(),
                    'sender': self,
                    'sid': config['sad'],
                    'payload': item,
                    'topic': config['topic']
                }
