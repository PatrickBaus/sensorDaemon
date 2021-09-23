# -*- coding: utf-8 -*-
"""
This file contains the host, that discovers its sensors and will then configure
them and aggregate the sensor data.
"""
import asyncio
import logging

from aiostream import stream, pipe

from data_types import AddChangeEvent, RemoveChangeEvent
from .gpib import PrologixGpibSensor, EVENT_BUS_DISCONNECT_BY_UID as EVENT_BUS_SENSOR_DISCONNECT_BY_UID
from .sensor_host import SensorHost, EVENT_BUS_CONFIG_UPDATE, EVENT_BUS_ADD_SENSOR, EVENT_BUS_ADD_HOST as EVENT_BUS_HOST_ADD_HOST, EVENT_BUS_DISCONNECT as EVENT_BUS_HOST_DISCONNECT


class PrologixGpibSensorHost(SensorHost):
    """
    Class that wraps a Tinkerforge brick daemon host. This can be either a
    PC running brickd, or a master brick with an ethernet/wifi extension.
    """
    @property
    def driver(self):
        return 'prologix_gpib'

    def __init__(self, uuid, hostname, port, reconnect_interval=3):
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
        self.__sensors = set()
        self.__reconnect_interval = reconnect_interval

    def __repr__(self):
        return f"{self.__class__.__module__}.{self.__class__.__qualname__}(hostname={self.hostname}, port={self.port})"

    async def __aenter__(self):
        # Do nothing, because GPIB does not have enumeration capabilities.
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        pass

    def disconnect(self, event_bus):
        """
        Disconnet the host and all sensors. Call only if the sensor is physically
        disconnected.

        Parameters
        ----------
        event_bus: AsyncEventBus
            The event bus to publish the disconnect event to
        """
        return

    async def __sensor_data_producer(self, sensor, event_bus, ping_interval):
        # TODO: Add message here
        self.__sensors.add(sensor)
        try:
            async with sensor as gpib_device:
                async for event in gpib_device.read_events(event_bus, ping_interval=ping_interval):
                    yield event
        finally:
            self.__sensors.remove(sensor)

    async def __update_listener(self, event_bus):
        """
        If we receive an update, that chages either the driver or the remote
        connection, we will tear down this host and replace it with a new one.

        Parameters
        ----------
        event_bus: AsyncEventBus
            The event bus to listen for updates
        Returns
        -------
        Iterartor[RemoveChangeEvent]
            Yields a RemoveChangeEvent, if there was an update, that requires a new connection.
        """
        async for event in event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE.format(uuid=self.uuid)):
            if event.change.driver != self.driver or event.change.hostname != self.hostname or event.change.port != self.port:
                event_bus.publish(EVENT_BUS_HOST_ADD_HOST, AddChangeEvent(event.change))  # Create a new host via the manager
                yield RemoveChangeEvent()   # Terminate this host
            # else do nothing for now

    async def __sensor_producer(self, event_bus):
        gen = await event_bus.call("/database/gpib/get_sensors", self.uuid)
        async for sensor_config in gen:
            sensor = PrologixGpibSensor(
                hostname=self.hostname,
                port=self.port,
                pad=sensor_config['pad'],
                sad=sensor_config['sad'],
                uuid=sensor_config['id'],
                reconnect_interval=self.__reconnect_interval,
            )
            yield AddChangeEvent(sensor)

        async for event in event_bus.subscribe(EVENT_BUS_ADD_SENSOR.format(uuid=self.uuid)):
            sensor_config = event.change
            sensor = PrologixGpibSensor(
                hostname=self.hostname,
                port=self.port,
                pad=sensor_config['pad'],
                sad=sensor_config['sad'],
                uuid=sensor_config['id'],
                reconnect_interval=self.__reconnect_interval,
            )
            yield AddChangeEvent(sensor)

    @staticmethod
    def is_no_event(event_type):
        def filter_event(item):
            return not isinstance(item, event_type)

        return filter_event

    async def read_data(self, event_bus):
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
        new_sensors_queue = asyncio.Queue()  # Add generators here to add them to the output
        new_sensors_queue.put_nowait(self.__sensor_producer(event_bus))
        new_sensors_queue.put_nowait(self.__update_listener(event_bus))
        new_sensors_queue.put_nowait(event_bus.subscribe(EVENT_BUS_HOST_DISCONNECT.format(uuid=self.uuid)))

        # For details on using aiostream, check here:
        # https://aiostream.readthedocs.io/en/stable/core.html#stream-base-class
        merged_stream = stream.call(new_sensors_queue.get) | pipe.cycle() | pipe.flatten() | pipe.takewhile(self.is_no_event(RemoveChangeEvent))   # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                if isinstance(item, AddChangeEvent):
                    new_sensors_queue.put_nowait(self.__sensor_data_producer(item.change, event_bus, ping_interval=5))
                else:
                    yield item
