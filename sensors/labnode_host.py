# -*- coding: utf-8 -*-
"""
This file contains the host, that discovers its sensors and will then configure
them and aggregate the sensor data.
"""
import asyncio
import logging
import time

from aiostream import stream, pipe

from labnode_async import IPConnection
from labnode_async.errors import InvalidCommandError

from data_types import ChangeEvent, AddChangeEvent, UpdateChangeEvent
from errors import DisconnectedDuringConnectError
from .sensor_host import SensorHost, EVENT_BUS_CONFIG_UPDATE, EVENT_BUS_ADD_HOST as EVENT_BUS_HOST_ADD_HOST, EVENT_BUS_DISCONNECT as EVENT_BUS_HOST_DISCONNECT

# Event bus topics
EVENT_BUS_BASE = "/sensors/labnode"
EVENT_BUS_CONFIG_UPDATE_BY_UID = EVENT_BUS_BASE + "/by_uid/{uid}/update"
EVENT_BUS_DISCONNECT_BY_UID = EVENT_BUS_BASE + "/by_uid/{uid}/disconnect"

MAXIMUM_FAILED_CONNECTIONS = 3


class LabnodeSensorHost(SensorHost):
    """
    Class that wraps a Labnode host.
    """
    @classmethod
    @property
    def driver(cls):
        return 'labnode'

    def __init__(self, uuid, hostname, port, event_bus, reconnect_interval=3):  # pylint: disable=too-many-arguments
        """
        Create new sensorHost Object.

        Parameters
        ----------
        hostname: str
            IP or hostname of the machine hosting the sensor daemon
        port: int
            port on the host
        reconnect_interval
        """
        super().__init__(uuid, hostname, port)
        self.__logger = logging.getLogger(__name__)
        self.__shutdown_event = asyncio.Event()
        self.__shutdown_event.set()   # Force the use of __aenter__()
        self.__sensor = None    # Will be set during __aenter__()
        self.__event_bus = event_bus
        self.__reconnect_interval = reconnect_interval

    def __repr__(self):
        return f"{self.__class__.__module__}.{self.__class__.__qualname__}(hostname={self.hostname}, port={self.port})"

    async def __aenter__(self):
        self.__event_bus.register(EVENT_BUS_HOST_DISCONNECT.format(uuid=self.uuid), self.__disconnect)
        self.__shutdown_event.clear()
        failed_connection_attemps = 0
        ipcon = IPConnection(
            host=self.hostname,
            port=self.port,
            timeout=None
        )
        while "not connected":
            try:
                pending = set()     # A set of pending tasks
                is_disconnected_task = asyncio.create_task(self.__shutdown_event.wait())
                pending.add(is_disconnected_task)
                connect_task = asyncio.create_task(self.__connect(ipcon))
                pending.add(connect_task)
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task == is_disconnected_task:    # pylint: disable=no-else-raise
                        await ipcon.disconnect()
                        raise DisconnectedDuringConnectError()
                    elif task == connect_task:
                        is_disconnected_task.cancel()
                        self.__sensor = task.result()   # If there is an error, blow up now
                        return self
            except (ConnectionError, OSError) as exc:
                failed_connection_attemps += 1
                # Suppress the warning after MAXIMUM_FAILED_CONNECTIONS to stop spamming log files
                if failed_connection_attemps < MAXIMUM_FAILED_CONNECTIONS:
                    if failed_connection_attemps > 1:
                        failure_count = f" ({failed_connection_attemps} times)"
                    else:
                        failure_count = ""
                    self.__logger.warning("Failed to connect to host '%s:%i'%s. Error: %s.", self.hostname, self.port, failure_count, exc)
                if failed_connection_attemps == MAXIMUM_FAILED_CONNECTIONS:
                    self.__logger.warning("Failed to connect to host '%s:%i' (%d time%s). Error: %s. Suppressing warnings from hereon.", self.hostname, self.port, failed_connection_attemps, "s"[failed_connection_attemps == 1:], exc)
                await asyncio.sleep(self.__reconnect_interval)

    async def __aexit__(self, exc_type, exc, traceback):
        self.__event_bus.unregister(EVENT_BUS_HOST_DISCONNECT.format(uuid=self.uuid))
        self.__shutdown_event.set()

    @staticmethod
    async def __connect(ipcon):
        await ipcon.connect()
        return await ipcon._get_device()

    async def __disconnect(self):
        self.__shutdown_event.set()

    async def __update_listener(self):
        """
        If we receive an update, that chages either the driver or the host
        connection, we will tear down this host and replace it with a new one.
        """
        async for event in self.__event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE.format(uuid=self.uuid)):
            if event.change.driver != self.driver or event.change.hostname != self.hostname or event.change.port != self.port:
                # If the driver, host or port changes, we will disconnect this host and let the manager create a new one
                self.__event_bus.publish(EVENT_BUS_HOST_ADD_HOST, AddChangeEvent(event.change))  # Create a new host via the manager
                self.__shutdown_event.set()   # Terminate this host
                break
            # else do nothing for now

    async def __configure(self, device, config):
        # Run on connect commands
        for cmd in config.get("on_connect", []):
            try:
                function = getattr(device, cmd['function'])
            except AttributeError:
                self.__logger.error("Invalid configuration parameter '%s' for sensor %s", cmd['function'], device)
                continue

            try:
                result = function(*cmd.get('args', []), **cmd.get('kwargs', {}))
                if asyncio.iscoroutine(result):
                    await result
            except Exception:   # pylint: disable=broad-except
                # Catch all exceptions and log them, because this is an external input
                self.__logger.exception("Error processing config for device on '%s'", self)
                continue

        return {(int(sid), sid_config['interval']/1000, sid_config['topic']) for sid, sid_config in config.get('config', {}).items()}

    async def __read(self, sensor, sid, interval, topic):
        while "sensor is connected":
            start = asyncio.get_running_loop().time()
            try:
                result = await sensor.get_by_function_id(sid)
            except InvalidCommandError:
                # We have an invalid function id
                self.__logger.error("Tried to execute invalid command '%d' on device '%s'", sid, self)
                return  # drop this
            yield {
                'timestamp': time.time(),
                'payload': result,
                'sid': sid,
                'topic': topic
            }
            nap_time = interval + start-asyncio.get_running_loop().time()
            if nap_time > 0:
                await asyncio.sleep(nap_time)

    async def __configure_and_read(self, sensor, sensor_config):
        if not sensor_config:
            return  # If we have an empty dict, there is nothing to read or configure
        configured_sids = await self.__configure(sensor, sensor_config)
        merged_stream = stream.merge(
            stream.iterate(self.__event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE_BY_UID.format(uid=sensor_config['uid']))),
            *[self.__read(sensor, *sid_config) for sid_config in configured_sids]
        )
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                if isinstance(item, ChangeEvent):
                    break   # The config has changed and we need stop reading an reconfigure the sensor
                yield item

    async def __config_producer(self, sensor):
        sensor_uid = await sensor.get_serial()
        initial_config = await self.__event_bus.call("/database/labnode/get_sensor_config", sensor_uid)
        yield AddChangeEvent(initial_config)

        async for event in self.__event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE_BY_UID.format(uid=sensor_uid)):
            yield event

    @staticmethod
    async def ping_host(device):
        """
        Regularly ping the host to make sure, the connection is still up and running.
        """
        while "device connected":
            await asyncio.sleep(0.1)    # TODO: Implement ping

    async def read_data(self):
        """
        Returns all data from all configured sensors connected to the host.

        Parameters
        ----------
        event_bus: AsyncEventBus
            The event bus passed to the sensors.

        Returns
        -------
        Iterartor[dict]
            The sensor data.
        """
        new_streams_queue = asyncio.Queue()  # Add generators here to add them to the output
        new_streams_queue.put_nowait(stream.iterate(self.__config_producer(self.__sensor)))    # sensor updates
        new_streams_queue.put_nowait(stream.just(self.__update_listener()))     # host updates
        new_streams_queue.put_nowait(stream.just(self.__shutdown_event.wait()))

        # For details on using aiostream, check here:
        # https://aiostream.readthedocs.io/en/stable/core.html#stream-base-class
        merged_stream = stream.call(new_streams_queue.get) | pipe.cycle() | pipe.flatten()   # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                if self.__shutdown_event.is_set():
                    break
                if isinstance(item, (AddChangeEvent, UpdateChangeEvent)):
                    new_streams_queue.put_nowait(stream.iterate(self.__configure_and_read(self.__sensor, item.change)))
                else:
                    item['sender'] = self
                    yield item
