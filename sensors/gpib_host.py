# -*- coding: utf-8 -*-
"""
This file contains the host, that discovers its sensors and will then configure
them and aggregate the sensor data.
"""
import asyncio
import logging

from aiostream import stream, pipe

from .gpib import PrologixGpibSensor, EVENT_BUS_DISCONNECT_BY_UID as EVENT_BUS_SENSOR_DISCONNECT_BY_UID
from .sensor_host import SensorHost, EVENT_BUS_CONFIG_UPDATE, EVENT_BUS_ADD_SENSOR


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
        self.__sensors = {}
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
        for sensor in self.__sensors.values():
            event_bus.emit(
                EVENT_BUS_SENSOR_DISCONNECT_BY_UID.format(uid=sensor.uid),
                None)
        self.__sensors = {}

    async def __sensor_data_producer(self, sensor, event_bus, ping_interval):
        self.__sensors[sensor.uid] = sensor
        try:
            async with sensor as gpib_device:
                async for event in gpib_device.read_events(event_bus, ping_interval=ping_interval):
                    yield event
        finally:
            self.__sensors.pop(sensor.uid, None)

    async def __update_listener(self, event_bus):
        async for _ in event_bus.register(EVENT_BUS_CONFIG_UPDATE.format(uuid=self.uuid)):
            self.__logger.error("Implement host config changes")

    async def __sensor_producer(self, event_bus):
        gen = await event_bus.call("/database/gpib/get_sensors", self.uuid)
        async for sensor_config in gen:
            sensor = PrologixGpibSensor(
                hostname=self.hostname,
                port=self.port,
                pad=sensor_config['pad'],
                sad=sensor_config['sad'],
                uid=sensor_config['uid'],
                reconnect_interval=self.__reconnect_interval,
            )
            yield sensor
        async for event in event_bus.subscribe(EVENT_BUS_ADD_SENSOR.format(uuid=self.uuid)):
            sensor_config = event.change
            sensor = PrologixGpibSensor(
                hostname=self.hostname,
                port=self.port,
                pad=sensor_config['pad'],
                sad=sensor_config['sad'],
                uid=sensor_config['uid'],
                reconnect_interval=self.__reconnect_interval,
            )
            yield sensor

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
        # TODO: Add host updates via __update_listener()
        new_sensors_queue = asyncio.Queue()  # Add generators here to add them to the output
        new_sensors_queue.put_nowait(self.__sensor_producer(event_bus))

        # For details on using aiostream, check here:
        # https://aiostream.readthedocs.io/en/stable/core.html#stream-base-class
        merged_stream = stream.call(new_sensors_queue.get) | pipe.cycle() | pipe.flatten()   # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                if isinstance(item, PrologixGpibSensor):
                    new_sensors_queue.put_nowait(self.__sensor_data_producer(item, event_bus, ping_interval=5))
                else:
                    yield item
