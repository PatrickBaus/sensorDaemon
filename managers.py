# -*- coding: utf-8 -*-
"""
This file contains the implementation of the managers for the sensor hosts. All
hosts of a certain type are managed by their managers. The mangers configure the
hosts extract the data stream from them.
"""
import asyncio
from contextlib import AsyncExitStack
import logging

from asyncio_mqtt import Client
import simplejson as json

from async_event_bus import AsyncEventBus
from databases import MongoDb, ChangeType
from helper_functions import cancel_all_tasks
from sensors.host_factory import host_factory
from sensors.tinkerforge import EVENT_BUS_CONFIG_UPDATE_BY_UID as EVENT_BUS_SENSOR_CONFIG_UPDATE, EVENT_BUS_STATUS as EVENT_BUS_SENSOR_STATUS
from sensors.tinkerforge_host import DisconnectedDuringConnectError, EVENT_BUS_CONFIG_UPDATE as EVENT_BUS_HOST_CONFIG_UPDATE, EVENT_BUS_DISCONNECT as EVENT_BUS_HOST_DISCONNECT, EVENT_BUS_BASE as EVENT_BUS_HOST_BASE


EVENT_BUS_DATA = "/sensor_data/tinkerforge"
MQTT_DATA_TOPIC = "sensors/tinkerforge/{uid}/{sid}"


class HostManager():
    """
    The Tinkerforge host manager responsible to start the tinkerforge sensor
    hosts.
    """
    def __init__(self, database_url, mqtt_host, mqtt_port):
        self.__logger = logging.getLogger(__name__)
        self.__database_url = database_url
        self.__mqtt_host = mqtt_host
        self.__mqtt_port = mqtt_port

    @staticmethod
    async def host_disconnect_listener(host, event_bus):
        """
        Waits for the event signalling a disconnect of the host. It will return
        if it recieved the data on the EVENT_BUS_HOST_DISCONNECT topic.

        Parameters
        ----------
        host: TinkerforgeSensorHost
            The host to watch.
        event_bus: AsyncEventBus
            The event bus to register to.
        """
        async for _ in event_bus.register(EVENT_BUS_HOST_DISCONNECT.format(uuid=host.uuid)):
            host.disconnect(event_bus)
            break

    async def host_reader(self, host, event_bus, reconnect_interval=3):
        """
        Connect to the host and read its data. Then push the data onto the event
        bus. Returns if the host shuts down the generator.

        Parameters
        ----------
        host: TinkerforgeSensorHost
            The host to read data from.
        event_bus: AsyncEventBus
            The event bus to push data to.
        reconnect_interval: int, default=3
            The time in seconds to wait between connection attempts.
        """
        disconnect_task = asyncio.create_task(
            self.host_disconnect_listener(host, event_bus),
            name=f"TF host {host.hostname}:{host.port} disconnector"
        )
        try:
            while "host not connected":
                try:
                    # Connect to the host using a context manager
                    async with host as host:
                        async for data in host.read_data(event_bus):
                            event_bus.emit(EVENT_BUS_DATA, data)
                        return
                except DisconnectedDuringConnectError:
                    break
                except Exception:   # pylint: disable=broad-except
                    # Catch all exceptions, log them, then try to restart the host.
                    self.__logger.exception("Error while reading data from host '%s:%i'. Reconnecting.", host.hostname, host.port)
                    await asyncio.sleep(reconnect_interval)
        finally:
            disconnect_task.cancel()
            try:
                await disconnect_task
            except asyncio.CancelledError:
                pass

    async def cancel_tasks(self, tasks):
        """
        Cancel all tasks and log any exceptions raised.

        Parameters
        ----------
        tasks: Iterable[asyncio.Task]
            The tasks to cancel
        """
        try:
            await cancel_all_tasks(tasks)
        except Exception:   # pylint: disable=broad-except
            self.__logger.exception("Error during shutdown of the host manager")

    async def database_host_config_publisher(self, database, event_bus):
        """
        Generates all changes to the hosts. It signals a new host
        to the host manager and config updates/removals to the host.

        Parameters
        ----------
        database: MongoDb
            The database to monitor.
        event_bus: AsyncEventBus
            The event bus to push data to.
        """
        async for host_config in database.get_hosts():
            host = host_factory.get(**host_config)
            event_bus.emit(EVENT_BUS_HOST_BASE, host)

        async for change_type, change in database.monitor_host_changes(timeout=5):      # TODO: Make configurable
            if change_type is ChangeType.UPDATE:
                event_bus.emit(EVENT_BUS_HOST_CONFIG_UPDATE.format(uuid=change['uid']), change)
            elif change_type is ChangeType.ADD:
                host = host_factory.get(**change, parent=self)
                event_bus.emit(EVENT_BUS_HOST_BASE, host)
            if change_type is ChangeType.REMOVE:
                event_bus.emit(EVENT_BUS_HOST_DISCONNECT.format(uuid=change), None)

    async def database_sensor_config_worker(self, database, event_bus, sensors):
        """
        Monitors the event bus for new sensors and then sends out a
        config update to the topic registered by the sensor.

        Parameters
        ----------
        database: MongoDb
            The database to monitor.
        event_bus: AsyncEventBus
            The event bus to push data to.
        sensors: dict[int, int]
            A dict, that stores the relationship of the config uuids and their
            corresponding sensor uid.
        """
        async for uid in event_bus.register(EVENT_BUS_SENSOR_STATUS):
            config = await database.get_sensor_config(uid)
            if config is not None:
                sensors[(config['id'])] = config['uid']
                event_bus.emit(EVENT_BUS_SENSOR_CONFIG_UPDATE.format(uid=uid), config)

    async def database_sensor_config_publisher(self, database, event_bus, sensors):
        """
        Monitors the database for sensors config changes and then sends out a
        config update to the topic registered by the sensor.

        Parameters
        ----------
        database: MongoDb
            The database to monitor.
        event_bus: AsyncEventBus
            The event bus to push data to.
        sensors: dict[int, int]
            A dict, that stores the relationship of the config uuids and their
            corresponding sensor uid.
        """
        changes = database.monitor_sensor_changes(timeout=5)
        async for change_type, change in changes:
            if change_type in (ChangeType.ADD, ChangeType.UPDATE):
                sensor = sensors.get(change['id'])
                if sensor is not None:
                    if sensor != change['uid']:
                        # The sensor uid was changed for the current config -> remove the old sensor config
                        event_bus.emit(EVENT_BUS_SENSOR_CONFIG_UPDATE.format(uid=sensor), {})
                sensors[(change['id'])] = change['uid']
                event_bus.emit(EVENT_BUS_SENSOR_CONFIG_UPDATE.format(uid=change['uid']), change)
            elif change_type is ChangeType.REMOVE:
                # change is the config uuid
                sensor = sensors.get(change)
                if sensor is not None:
                    event_bus.emit(EVENT_BUS_SENSOR_CONFIG_UPDATE.format(uid=sensor), {})
                    del sensors[change]

    async def mqtt_producer(self, event_bus, output_queue):
        """
        Grabs the output data from the event bus and pushes it to a worker queue,
        so that multiple workers can then publish it via MQTT.

        Parameters
        ----------
        event_bus: AsyncEventBus
            The event bus to push data to.
        output_queue: asyncio.Queue
            The output queue, to aggregate the data to.
        """
        async for event in event_bus.register(EVENT_BUS_DATA):
            payload = {
                'timestamp': event['timestamp'],
                'uid': event['sender'].uid,
                'sid': event['sid'],
                'value': event['payload'],
            }
            output_queue.put_nowait(payload)

    async def mqtt_consumer(self, input_queue, reconnect_interval=5):
        """
        Pushes the data from the input queue to the MQTT broker. It will make sure,
        that no data is lost if the MQTT broker disconnects.

        Parameters
        ----------
        input_queue: asyncio.Queue
            The queue, that supplies the worker with data
        reconnect_interval: int, default=5
            The time in seconds to wait between connection attempts.
        """
        while "loop not cancelled":
            self.__logger.info("Connecting worker to MQTT broker at '%s:%i", self.__mqtt_host, self.__mqtt_port)
            event = None
            try:
                async with Client(hostname=self.__mqtt_host, port=self.__mqtt_port) as mqtt_client:
                    while "loop not cancelled":
                        if event is None:
                            # only get new data if we have pused all to the broker
                            event = await input_queue.get()
                        try:
                            # convert payload to JSON
                            # Typically sensors return data as decimals or ints to preserve the precision
                            payload = json.dumps(event, use_decimal=True)
                            print("mqtt_consumer", payload)
                            await mqtt_client.publish(
                                MQTT_DATA_TOPIC.format(
                                    uid=event['uid'],
                                    sid=event['sid']
                                ),
                                payload=payload,
                                qos=1
                            )
                            event = None    # Get a new event to publish
                        finally:
                            input_queue.task_done()
            except Exception:   # pylint: disable=broad-except
                # Catch all exceptions, log them, then try to restart the worker.
                self.__logger.exception("Error while publishing data to MQTT broker. Reconnecting.")
                await asyncio.sleep(reconnect_interval)

    async def host_config_producer(self, event_bus, output_queue):
        """
        Retrieve all added and removed hosts from the bus and put them into a
        queue.

        Parameters
        ----------
        event_bus: AsyncEventBus
            The event bus to retrieve data from.
        output_queue: asyncio.Queue
            The output queue, to aggregate the data to.
        """
        async for value in event_bus.register(EVENT_BUS_HOST_BASE):
            output_queue.put_nowait(value)

    async def host_config_consumer(self, event_queue, event_bus):
        """
        The config consumer, takes a host from the `event_queue` and schedules a
        task to read its data. There is no cleanup done, the host must clean up
        its own mess.

        Parameters
        ----------
        event_queue: asyncio.Queue
            The input queue, that has all new hosts waiting to run.
        event_bus: AsyncEventBus
            The event bus used by a host to register for status updates.
        """
        pending = set()
        add_host_task = asyncio.create_task(event_queue.get())
        pending.add(add_host_task)
        # TODO: Keep a list of hosts to make sure there are no duplicates.

        try:
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task == add_host_task:
                        try:
                            host = task.result()
                            new_task = asyncio.create_task(self.host_reader(host, event_bus))
                            pending.add(new_task)
                        finally:
                            event_queue.task_done()
                        add_host_task = asyncio.create_task(event_queue.get())
                        pending.add(add_host_task)
                    # else: A terminated host job, all exceptions are caught in the task
        finally:
            # If there are remaining tasks, kill them
            await cancel_all_tasks(pending)

    async def mqtt_worker(self, event_bus, number_of_publishers=5):
        """
        The MQTT worker, that gets the data from the event bus and publishes it
        to the MQTT broker.

        Parameters
        ----------
        event_bus: AsyncEventBus
            The event bus that the sensors are publishing to.
        number_of_publishers: int
            The number of workers, pushing data from the event bus to the MQTT
            broker.
        """
        async with AsyncExitStack() as stack:
            tasks = set()
            stack.push_async_callback(self.cancel_tasks, tasks)
            event_queue = asyncio.Queue()

            consumers = {asyncio.create_task(self.mqtt_consumer(event_queue)) for i in range(number_of_publishers)}
            tasks.update(consumers)

            task = asyncio.create_task(self.mqtt_producer(event_bus, event_queue))
            tasks.add(task)

            await asyncio.gather(*tasks)

    async def host_worker(self, event_bus, number_of_publishers=5):
        """
        The worker, that creates hosts from configs.

        Parameters
        ----------
        event_bus: AsyncEventBus
            The event bus that the database publishes updates to.
        number_of_publishers: int
            The number of workers configuring new hosts
            broker.
        """
        async with AsyncExitStack() as stack:
            tasks = set()
            stack.push_async_callback(self.cancel_tasks, tasks)
            event_queue = asyncio.Queue()

            consumers = {asyncio.create_task(self.host_config_consumer(event_queue, event_bus)) for i in range(number_of_publishers)}
            tasks.update(consumers)

            task = asyncio.create_task(self.host_config_producer(event_bus, event_queue))
            tasks.add(task)

            await asyncio.gather(*tasks)

    async def database_worker(self, event_bus, reconnect_interval=3):
        """
        The worker, that retrieves data from the database and pushes it to the
        event bus. It also publishes updates on the event bus.

        Parameters
        ----------
        event_bus: AsyncEventBus
            The event bus, that both the hosts and the sensors listen to.
        """
        while "loop not cancelled":
            # TODO: Notify all hosts/sensors, when the database is connected
            try:
                async with AsyncExitStack() as stack:
                    tasks = set()
                    stack.push_async_callback(self.cancel_tasks, tasks)

                    database_driver = MongoDb(self.__database_url)
                    database = await stack.enter_async_context(database_driver)
                    sensors = {}
                    task = asyncio.create_task(self.database_sensor_config_worker(database, event_bus, sensors))
                    tasks.add(task)

                    task = asyncio.create_task(self.database_sensor_config_publisher(database, event_bus, sensors))
                    tasks.add(task)

                    task = asyncio.create_task(self.database_host_config_publisher(database, event_bus))
                    tasks.add(task)

                    await asyncio.gather(*tasks)
            except Exception:   # pylint: disable=broad-except
                # Catch all exceptions, log them, then try to restart the worker.
                self.__logger.exception("Error while processing database.")
                asyncio.sleep(reconnect_interval)

    async def run(self):
        """
        The main task, that spawn all workers.
        """
        async with AsyncExitStack() as stack:
            tasks = set()
            stack.push_async_callback(self.cancel_tasks, tasks)
            event_bus = AsyncEventBus()

            task = asyncio.create_task(self.database_worker(event_bus), name="Tinkerforge config database worker")
            tasks.add(task)

            task = asyncio.create_task(self.host_worker(event_bus), name="Tinkerforge host host configuration worker")
            tasks.add(task)

            task = asyncio.create_task(self.mqtt_worker(event_bus), name="MQTT data publisher")
            tasks.add(task)

            await asyncio.gather(*tasks)
