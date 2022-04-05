# -*- coding: utf-8 -*-
"""
This file contains the host, that discovers its sensors and will then configure
them and aggregate the sensor data.
"""
import asyncio
import logging

from aiostream import stream, pipe

from connections import IpConnection
from data_types import AddChangeEvent, DataEvent, UpdateChangeEvent, RemoveChangeEvent, RestartChangeEvent
from .drivers.generic_scpi_device import IncompatibleDeviceException
from .sensor_factory import sensor_factory
from .sensor_host import SensorHost


class GenericSensorHost(SensorHost):
    """
    Class that represents a generic Ethernet host.
    """
    @property
    def connection(self):
        return self.__connection

    @property
    def driver(self):
        return "generic_sensor"

    @property
    def reconnect_interval(self):
        return self.__reconnect_interval

    def __init__(self, uuid, connection, event_bus, reconnect_interval=3):  # pylint: disable=too-many-arguments
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
        super().__init__(uuid)
        self.__logger = logging.getLogger(__name__)
        self.__shutdown_event = asyncio.Event()
        self.__shutdown_event.set()   # Force the use of __aenter__()
        self.__sensors = dict()
        self.__event_bus = event_bus
        self.__connection = connection
        self.__reconnect_interval = reconnect_interval
        self.__retry_count = 0  # Used by the context manager to track the number of retries

    async def __aenter__(self):
        if self.__retry_count < 3:
            self.__logger.info("Connecting to %s.", self.__connection)
        self.__shutdown_event.clear()
        self.__retry_count += 1
        await self.__connection.connect()

        self.__retry_count = 0
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.__logger.info("Disconnecting from %s.", self.__connection)
        # self.__shutdown_event will normally already be set if shutting down gracefully
        # Set it again to make sure, that it is set even in case of an exception.
        await self.__connection.disconnect()
        self.__shutdown_event.set()

    async def __disconnect(self):
        self.__shutdown_event.set()

    async def __sensor_data_producer(self, sensor, config):
        if sensor.uuid in self.__sensors:
            self.__logger.error("Instantiated %s twice! Aborting", sensor)
            return

        self.__sensors[sensor.uuid] = sensor
        try:
            async with sensor as device:
                await device.configure(config)
                async for data in device.read_device():
                    yield data
        except IncompatibleDeviceException:
            self.__logger.warning("Device on host %s is not supported.", self)
        finally:
            self.__sensors.pop(sensor.uuid, None)

    def __create_sensor(self, config):
        if config['enabled']:
            try:
                sensor = sensor_factory.get(
                    uuid=config['id'],
                    driver=config['driver'],
                    connection=self.connection,
                    event_bus=self.__event_bus
                )
                return AddChangeEvent((sensor, config))
            except ValueError:
                self.__logger.warning("Driver '%s' for sensor '%s' not found", config['driver'], config['id'])

    async def __update_listener(self):
        """
        Listens for updates of any connected sensors
        """
        event_topic = f"devices/by_uid/{self.uuid}/update"
        self.__logger.debug("%s listening to %s", self, event_topic)
        async for event in self.__event_bus.subscribe(event_topic):
            if isinstance(event, UpdateChangeEvent):
                pass    # TODO: need to do something
            elif isinstance(event, AddChangeEvent):
                if event.change['id'] in self.__sensors:
                    self.__event_bus.publish(f"devices/by_uid/{event.change['id']}/update", RemoveChangeEvent())
                sensor = self.__create_sensor(event.change)
                if sensor:
                    yield sensor

    async def __sensor_producer(self):
        config = await self.__event_bus.call("sensors/ethernet/get_sensor_config", self.uuid)
        sensor = self.__create_sensor(config)
        if sensor:
            yield sensor

        async for update in self.__update_listener():
            yield update

    async def read_data(self):
        """
        Returns all data from all configured sensors connected to the host.

        Returns
        -------
        Iterator[DataEvent]
            The sensor data.
        """
        new_streams_queue = asyncio.Queue()  # Add generators here to add them to the output
        new_streams_queue.put_nowait(stream.iterate(self.__sensor_producer()))  # Generates new sensors for the initial config and if the config changes
        #new_streams_queue.put_nowait(stream.iterate(self.__update_listener()))  # Sets the shutdown event if the driver, hostname or port of this config is changed
        new_streams_queue.put_nowait(stream.just(self.__shutdown_event.wait()))

        # For details on using aiostream, check here:
        # https://aiostream.readthedocs.io/en/stable/core.html#stream-base-class
        merged_stream = stream.call(new_streams_queue.get) | pipe.cycle() | pipe.flatten()   # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                if self.__shutdown_event.is_set():
                    break
                if isinstance(item, DataEvent):
                    yield item
                elif isinstance(item, AddChangeEvent):
                    # We have a new sensor. This can also be an existing sensor with a new config
                    new_streams_queue.put_nowait(stream.iterate(self.__sensor_data_producer(*item.change)))
                elif isinstance(item, UpdateChangeEvent):
                    pass


class EthernetSensorHost(GenericSensorHost):
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
        super().__init__(
            uuid,
            IpConnection(hostname, port, timeout=10),
            event_bus,
            reconnect_interval
        )

    def __str__(self):
        return f"ethernet sensor host ({self.connection.hostname}:{self.connection.port})"

    def __repr__(self):
        return f"{self.__class__.__module__}.{self.__class__.__qualname__}(hostname={self.connection.hostname}, port={self.connection.port})"
