# -*- coding: utf-8 -*-
"""
This file contains the host, that discovers its sensors and will then configure
them and aggregate the sensor data.
"""
import asyncio
import logging

from aiostream import stream, pipe
from tinkerforge_async.ip_connection import EnumerationType, IPConnectionAsync as IPConnection

from data_types import ChangeType
from errors import DisconnectedDuringConnectError
from .tinkerforge import TinkerforgeSensor, EVENT_BUS_DISCONNECT_BY_UID as EVENT_BUS_SENSOR_DISCONNECT_BY_UID
from .sensor_host import SensorHost, EVENT_BUS_CONFIG_UPDATE, EVENT_BUS_DISCONNECT as EVENT_BUS_HOST_DISCONNECT

MAXIMUM_FAILED_CONNECTIONS = 3


class TinkerforgeSensorHost(SensorHost):
    """
    Class that wraps a Tinkerforge brick daemon host. This can be either a
    PC running brickd, or a master brick with an ethernet/wifi extension.
    """
    @property
    def driver(self):
        return 'tinkerforge'

    def __init__(self, uuid, hostname, port, event_bus, reconnect_interval=3):
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
        self.__sensors = {}
        self.__ipcon = None
        self.__event_bus = event_bus
        self.__reconnect_interval = reconnect_interval

    def __repr__(self):
        return f"{self.__class__.__module__}.{self.__class__.__qualname__}(hostname={self.hostname}, port={self.port})"

    async def __aenter__(self):
        self.__logger.info("Connecting to Tinkerforge host '%s:%i'.", self.hostname, self.port)
        self.__event_bus.register(EVENT_BUS_HOST_DISCONNECT.format(uuid=self.uuid), self.__disconnect)
        if self.__ipcon is None:
            self.__ipcon = IPConnection(host=self.hostname, port=self.port)
        failed_connection_attemps = 0
        self.__shutdown_event.clear()
        while "not connected":
            try:
                pending = set()
                is_disconnected_task = asyncio.create_task(self.__shutdown_event.wait())
                pending.add(is_disconnected_task)
                connect_task = asyncio.create_task(self.__ipcon.connect())
                pending.add(connect_task)
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task == is_disconnected_task:    # pylint: disable=no-else-raise
                        await self.__ipcon.disconnect()
                        raise DisconnectedDuringConnectError()
                    else:
                        is_disconnected_task.cancel()
                        task.result()   # If there is an error, blow up now
                        return self
            except (OSError, asyncio.TimeoutError) as exc:
                # OSError includes ConnectionError which includes both
                # ip_connection.NetworkUnreachableError and
                # ip_connection.NotConnectedError
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
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.__event_bus.unregister(EVENT_BUS_HOST_DISCONNECT.format(uuid=self.uuid))
        self.__shutdown_event.set()
        await self.__ipcon.disconnect()

    async def __disconnect(self):
        self.__shutdown_event.set()

    async def __process_enumerations(self, ipconnection):
        """
        This infinite loop pulls events from the internal enumeration queue
        of the ip connection and waits for an enumeration event to create the devices.
        The devices are then passed on to __monitor_sensors() via a job_queue.
        """
        async for packet in ipconnection.read_enumeration():
            if packet['enumeration_type'] in (EnumerationType.CONNECTED, EnumerationType.AVAILABLE):
                try:
                    device = TinkerforgeSensor(packet['device_id'], packet['uid'], ipconnection, self)
                    yield (ChangeType.ADD, device)
                except ValueError:
                    # raised by the TinkerforgeSensor constructor (using the sensor factory) if a device_id cannot be found (i.e. there is no driver)
                    self.__logger.warning("Unsupported device id '%s' with uid '%s' found on host '%s'", packet["uid"], packet["device_id"], ipconnection.hostname)
            elif packet['enumeration_type'] is EnumerationType.DISCONNECTED:
                yield (ChangeType.REMOVE, packet['uid'])

    async def __sensor_data_producer(self, sensor, ping_interval):
        self.__sensors[sensor.uid] = sensor
        try:
            async for event in sensor.read_events(self.__event_bus, ping_interval=ping_interval):
                yield event
        finally:
            self.__sensors.pop(sensor.uid, None)

    async def __update_listener(self):
        async for _ in self.__event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE.format(uuid=self.uuid)):
            self.__logger.error("Implement host config changes")

    async def __sensor_producer(self, ipconnection):
        await self.__ipcon.enumerate()
        async for change_type, sensor in self.__process_enumerations(ipconnection):
            if change_type == ChangeType.ADD and sensor.uid not in self.__sensors:
                yield sensor
            elif change_type == ChangeType.REMOVE:
                self.__event_bus.publish(EVENT_BUS_SENSOR_DISCONNECT_BY_UID.format(uid=sensor))

    async def read_data(self):
        """
        Returns all data from all configured sensors connected to the host.

        Returns
        -------
        Iterartor[dict]
            The sensor data.
        """
        # TODO: Add host updates via __update_listener()
        new_sensors_queue = asyncio.Queue()  # Add generators here to add them to the output
        new_sensors_queue.put_nowait(self.__sensor_producer(self.__ipcon))
        new_sensors_queue.put_nowait(stream.just(self.__shutdown_event.wait()))

        # For details on using aiostream, check here:
        # https://aiostream.readthedocs.io/en/stable/core.html#stream-base-class
        merged_stream = stream.call(new_sensors_queue.get) | pipe.cycle() | pipe.flatten()  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                if self.__shutdown_event.is_set():
                    break
                if isinstance(item, TinkerforgeSensor):
                    new_sensors_queue.put_nowait(self.__sensor_data_producer(item, ping_interval=30))
                else:
                    yield item
