# -*- coding: utf-8 -*-
"""
This file contains the implementation of the managers for the sensor hosts. All
hosts of a certain type are managed by their managers. The mangers configure the
hosts extract the data stream from them.
"""
import asyncio
from contextlib import AsyncExitStack
import logging

import asyncio_mqtt
import simplejson as json

from async_event_bus import AsyncEventBus
from databases import MongoDb, CONTEXTS as DATABASE_CONTEXTS
from errors import DisconnectedDuringConnectError
from helper_functions import cancel_all_tasks
from sensors.sensor_host import EVENT_BUS_ADD_HOST as EVENT_BUS_HOST_ADD_HOST


EVENT_BUS_DATA = "/sensor_data/all"
MQTT_DATA_TOPIC = "sensors/{driver}/{uid}/{sid}"


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

    async def host_reader(self, host, event_bus, reconnect_interval=3):
        """
        Connect to the host and read its data. Then push the data onto the event
        bus. Returns if the host shuts down the generator.

        Parameters
        ----------
        host: SensorHost
            The host to read data from.
        event_bus: AsyncEventBus
            The event bus to push data to.
        reconnect_interval: int, default=3
            The time in seconds to wait between connection attempts.
        """
        while "host not connected":
            number_of_retries = 0
            try:
                number_of_retries += 1
                # Connect to the host using a context manager
                if number_of_retries <= 3:
                    self.__logger.info("Connecting to %s host (%s:%i).", host.driver, host.hostname, host.port)
                async with host as reader:
                    number_of_retries = 0
                    async for data in reader.read_data():
                        data['driver'] = host.driver
                        event_bus.publish(EVENT_BUS_DATA, data)
                    return
            except DisconnectedDuringConnectError:
                break
            except ConnectionError as exc:
                self.__logger.error("Connecting to %s host (%s:%i) lost. Reconnecting. Error: %s", host.driver, host.hostname, host.port, exc)
                await asyncio.sleep(reconnect_interval)
                continue
            except Exception:   # pylint: disable=broad-except
                # Catch all exceptions, log them, then try to restart the host.
                self.__logger.exception("Error while reading data from host '%s:%i'. Reconnecting.", host.hostname, host.port)
                await asyncio.sleep(reconnect_interval)

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
        async for event in event_bus.subscribe(EVENT_BUS_DATA):
            try:
                topic = event['topic']
                payload = {
                    'timestamp': event['timestamp'],
                    'uuid': str(event['sender'].uuid),
                    'driver': event['driver'],
                    'sid': event['sid'],
                    'value': event['payload'],
                }
            except Exception:   # pylint: disable=broad-except
                self.__logger.exception("Malformed data received. Dropping data: %s", event)
            else:
                output_queue.put_nowait((topic, payload))

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
        has_error = False
        self.__logger.info("Connecting worker to MQTT broker (%s:%i).", self.__mqtt_host, self.__mqtt_port)
        while "loop not cancelled":
            event = None
            try:
                async with asyncio_mqtt.Client(hostname=self.__mqtt_host, port=self.__mqtt_port) as mqtt_client:
                    while "loop not cancelled":
                        if event is None:   # TODO: Add a delay in the exception handler
                            # only get new data if we have pused all to the broker
                            event = await input_queue.get()
                        try:
                            topic, payload = event
                            # convert payload to JSON
                            # Typically sensors return data as decimals or ints to preserve the precision
                            payload = json.dumps(payload, use_decimal=True)
                            await mqtt_client.publish(topic, payload=payload, qos=2)
                            event = None    # Get a new event to publish
                            has_error = False
                        finally:
                            input_queue.task_done()
            except asyncio_mqtt.error.MqttError as exc:
                # Only log an error once
                if not has_error:
                    self.__logger.error("MQTT error: %s. Retrying.", exc)
                await asyncio.sleep(reconnect_interval)
                has_error = True
            except Exception:   # pylint: disable=broad-except
                # Catch all exceptions, log them, then try to restart the worker.
                self.__logger.exception("Error while publishing data to MQTT broker. Reconnecting.")
                await asyncio.sleep(reconnect_interval)

    @staticmethod
    async def host_config_producer(event_bus, output_queue):
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
        async for event in event_bus.subscribe(EVENT_BUS_HOST_ADD_HOST):
            output_queue.put_nowait(event.change)

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
                    await stack.enter_async_context(database_driver)
                    context_managers = await asyncio.gather(*[stack.enter_async_context(context(event_bus)) for context in DATABASE_CONTEXTS])
                    for context_manager in context_managers:
                        task = asyncio.create_task(context_manager.monitor_changes(timeout=5))
                        tasks.add(task)

                    await asyncio.gather(*tasks)
            except Exception:   # pylint: disable=broad-except
                # Catch all exceptions, log them, then try to restart the worker.
                self.__logger.exception("Error while processing database.")
                await asyncio.sleep(reconnect_interval)

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
