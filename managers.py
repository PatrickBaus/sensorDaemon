"""
This file contains the implementation of the managers for the sensor hosts. All
hosts of a certain type are managed by their managers. The mangers configure the
hosts extract the data stream from them.
"""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
import logging
from typing import Any, Set
from uuid import UUID

import asyncio_mqtt
import simplejson as json
from aiostream import stream, pipe

from async_event_bus import TopicNotRegisteredError, event_bus
from data_types import DataEvent
from databases import MongoDb, CONTEXTS as DATABASE_CONTEXTS
from errors import UnknownDriverError
from helper_functions import catch, iterate_safely, cancel_all_tasks
from sensors.transports.transport_factory import transport_factory

EVENT_BUS_DATA = "sensor_data/all"
MQTT_DATA_TOPIC = "sensors/{driver}/{uid}/{sid}"


class MqttManager:
    def __init__(self, host: str, port: int, number_of_workers: int = 5) -> None:
        self.__logger = logging.getLogger(__name__)
        self.__host = host
        self.__port = port
        self.__number_of_workers = number_of_workers

    async def producer(self, output_queue: asyncio.Queue[(str, dict[str, str | float | int])]):
        """
        Grabs the output data from the event bus and pushes it to a worker queue,
        so that multiple workers can then publish it via MQTT.

        Parameters
        ----------
        output_queue: asyncio.Queue
            The output queue, to aggregate the data to.
        """
        event: DataEvent
        async for event in event_bus.subscribe("wamp/publish"):
            try:
                # Events are DataEvents
                topic = event.topic
                payload = {
                    'timestamp': event.timestamp,
                    'uuid': str(event.sender),
                    'sid': event.sid,
                    'value': event.value,
                    'unit': event.unit
                }
            except Exception:   # pylint: disable=broad-except
                self.__logger.exception("Malformed data received. Dropping data: %s", event)
            else:
                output_queue.put_nowait((topic, payload))

    async def consumer(
            self,
            input_queue: asyncio.Queue[(str, dict[str, str | float | int])],
            reconnect_interval: int = 5
    ) -> None:
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
        self.__logger.info("Connecting worker to MQTT broker (%s:%i).", self.__host, self.__port)
        event = None
        while "loop not cancelled":
            try:
                async with asyncio_mqtt.Client(hostname=self.__host, port=self.__port) as mqtt_client:
                    while "loop not cancelled":
                        if event is None:
                            # only get new data if we have pushed everything to the broker
                            event = await input_queue.get()
                            input_queue.task_done()
                        try:
                            topic, payload = event
                            # convert payload to JSON
                            # Typically sensors return data as decimals or ints to preserve the precision
                            payload = json.dumps(payload, use_decimal=True)
                        except TypeError:
                            self.__logger.error("Error while serializing DataEvent: %s", payload)
                            event = None    # Drop the event
                            # await asyncio.sleep(0.01)
                        else:
                            #self.__logger.info("Going to publish: %s to %s", payload, topic)
                            await mqtt_client.publish(topic, payload=payload, qos=2)
                            event = None    # Get a new event to publish
                            has_error = False
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

    async def cancel_tasks(self, tasks: Set[asyncio.Task]):
        """
        Cancel all tasks and log any exceptions raised.

        Parameters
        ----------
        tasks: Set[asyncio.Task]
            The tasks to cancel
        """
        try:
            await cancel_all_tasks(tasks)
        except Exception:   # pylint: disable=broad-except
            self.__logger.exception("Error during shutdown of the MQTT manager")

    async def run(self) -> None:
        """
        The main task, that spawn all workers.
        """
        async with AsyncExitStack() as stack:
            tasks = set()
            stack.push_async_callback(self.cancel_tasks, tasks)
            event_queue = asyncio.Queue()

            consumers = {asyncio.create_task(self.consumer(event_queue)) for _ in range(self.__number_of_workers)}
            tasks.update(consumers)

            task = asyncio.create_task(self.producer(event_queue))
            tasks.add(task)

            await asyncio.gather(*tasks)


class DatabaseManager:
    def __init__(self, database_url: str) -> None:
        self.__logger = logging.getLogger(__name__)
        self.__database_url = database_url

    async def cancel_tasks(self, tasks: Set[asyncio.Task]) -> None:
        """
        Cancel all tasks and log any exceptions raised.

        Parameters
        ----------
        tasks: Set[asyncio.Task]
            The tasks to cancel
        """
        try:
            await cancel_all_tasks(tasks)
        except Exception:   # pylint: disable=broad-except
            self.__logger.exception(f"Error during shutdown of the {type(self).__name__}")

    async def run(self) -> None:
        """
        The main task, that spawn all workers.
        """
        while "loop not cancelled":
            # TODO: Notify all hosts/sensors, when the database is connected
            try:
                async with AsyncExitStack() as stack:
                    tasks = set()
                    stack.push_async_callback(self.cancel_tasks, tasks)

                    database_driver = MongoDb(self.__database_url)
                    await stack.enter_async_context(database_driver)
                    context_managers = await asyncio.gather(
                        *[stack.enter_async_context(context()) for context in DATABASE_CONTEXTS]
                    )
                    for context_manager in context_managers:
                        task = asyncio.create_task(
                            context_manager.monitor_changes(timeout=5), name="Host config database worker"
                        )
                        tasks.add(task)

                    await asyncio.gather(*tasks)
            except Exception:   # pylint: disable=broad-except
                # Catch all exceptions, log them, then try to restart the worker.
                self.__logger.exception("Error while processing database.")
                await asyncio.sleep(5)


class HostManager:
    def __init__(self, node_id: UUID) -> None:
        self.__node_id = node_id
        self.__logger = logging.getLogger(__name__)
        self.__topic = "db_autodiscovery_sensors"

    @staticmethod
    def _create_transport(config: dict[str, Any]):
        if config is None:
            return None
        try:
            return transport_factory.get(**config)
        except UnknownDriverError:
            logging.getLogger(__name__).warning(f"No driver available for transport '{config['driver']}'")
        except Exception:
            logging.getLogger(__name__).exception(f"Error while creating transport '{config['driver']}'")
        return None

    @staticmethod
    def _is_config_valid(node_id, config: dict[str, Any]) -> bool:
        return (
            config is not None
            and config['enabled']
            and (config['node_id'] is None or node_id is None or config['node_id'] == node_id)
        )

    async def start_stream(self) -> None:
        # Generate the UUIDs of new sensors
        sensor_stream = (
            stream.chain(
                stream.iterate(iterate_safely(f"{self.__topic}/get", f"{self.__topic}/status_update")),
                stream.iterate(event_bus.subscribe(f"{self.__topic}/add_host"))
            )
            | pipe.flatmap(
                lambda item: stream.chain(
                    (stream.call(event_bus.call, f"{self.__topic}/get_config", item)
                        | catch.pipe(TopicNotRegisteredError)),
                    stream.iterate(event_bus.subscribe(f"nodes/by_uuid/{item}/update"))
                )
                | pipe.until(lambda config: config is None)
                | pipe.map(lambda config: config if self._is_config_valid(self.__node_id, config) else None)
                | pipe.map(lambda config: self._create_transport(config))
                | pipe.switchmap(
                    lambda transport: stream.empty() if transport is None else stream.iterate(transport.stream_data())
                )
                | pipe.action(lambda data: event_bus.publish("wamp/publish", data))
            )
        )

        await sensor_stream

    async def run(self) -> None:
        await self.start_stream()
