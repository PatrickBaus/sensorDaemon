# -*- coding: utf-8 -*-
"""
This file contains the host, that discovers its sensors and will then configure
them and aggregate the sensor data.
"""
import asyncio
import logging

from aiostream import stream, pipe

from data_types import AddChangeEvent
from .gpib import PrologixGpibSensor
from .sensor_host import SensorHost, EVENT_BUS_CONFIG_UPDATE, EVENT_BUS_ADD_SENSOR, EVENT_BUS_ADD_HOST as EVENT_BUS_HOST_ADD_HOST, EVENT_BUS_DISCONNECT as EVENT_BUS_HOST_DISCONNECT


class PrologixGpibSensorHost(SensorHost):
    """
    Class that wraps a Prologix GPIB adapter.
    """
    @classmethod
    @property
    def driver(cls):
        return 'prologix_gpib'

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
        self.__sensors = set()
        self.__event_bus = event_bus
        self.__reconnect_interval = reconnect_interval

    def __repr__(self):
        return f"{self.__class__.__module__}.{self.__class__.__qualname__}(hostname={self.hostname}, port={self.port})"

    async def __aenter__(self):
        self.__logger.info("Connecting to Pologix adapter (%s:%i).", self.hostname, self.port)
        self.__event_bus.register(EVENT_BUS_HOST_DISCONNECT.format(uuid=self.uuid), self.__disconnect)
        self.__shutdown_event.clear()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.__shutdown_event.set()

    async def __disconnect(self):
        self.__shutdown_event.set()

    async def __sensor_data_producer(self, sensor, ping_interval):
        # TODO: Add message here
        self.__sensors.add(sensor)
        try:
            async with sensor as device:
                async for event in device.read_events(ping_interval=ping_interval):
                    yield event
        finally:
            self.__sensors.remove(sensor)

    async def __update_listener(self):
        """
        If we receive an update, that chages either the driver or the remote
        connection, we will tear down this host and replace it with a new one.
        """
        async for event in self.__event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE.format(uuid=self.uuid)):
            if event.change.driver != self.driver or event.change.hostname != self.hostname or event.change.port != self.port:
                self.__event_bus.publish(EVENT_BUS_HOST_ADD_HOST, AddChangeEvent(event.change))  # Create a new host via the manager
                self.__shutdown_event.set()   # Terminate this host
                break
            # else do nothing for now

    async def __sensor_producer(self):
        gen = await self.__event_bus.call("/database/gpib/get_sensors", self.uuid)
        async for sensor_config in gen:
            sensor = PrologixGpibSensor(
                hostname=self.hostname,
                port=self.port,
                pad=sensor_config['pad'],
                sad=sensor_config['sad'],
                uuid=sensor_config['id'],
                event_bus=self.__event_bus,
                reconnect_interval=self.__reconnect_interval,
            )
            yield AddChangeEvent(sensor)

        async for event in self.__event_bus.subscribe(EVENT_BUS_ADD_SENSOR.format(uuid=self.uuid)):
            sensor_config = event.change
            sensor = PrologixGpibSensor(
                hostname=self.hostname,
                port=self.port,
                pad=sensor_config['pad'],
                sad=sensor_config['sad'],
                uuid=sensor_config['id'],
                event_bus=self.__event_bus,
                reconnect_interval=self.__reconnect_interval,
            )
            yield AddChangeEvent(sensor)

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
        new_streams_queue.put_nowait(self.__sensor_producer())  # Generates new sensors for the initial config and if the config changes
        new_streams_queue.put_nowait(stream.just(self.__update_listener()))  # Sets the shutdown event if the driver, hostname or port of this config is changed
        new_streams_queue.put_nowait(stream.just(self.__shutdown_event.wait()))

        # For details on using aiostream, check here:
        # https://aiostream.readthedocs.io/en/stable/core.html#stream-base-class
        merged_stream = stream.call(new_streams_queue.get) | pipe.cycle() | pipe.flatten()   # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                if self.__shutdown_event.is_set():
                    break
                if isinstance(item, AddChangeEvent):
                    new_streams_queue.put_nowait(self.__sensor_data_producer(item.change, ping_interval=5))
                else:
                    yield item
