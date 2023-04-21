"""
This file contains the implementation of the managers for the sensor hosts. All
hosts of a certain type are managed by their managers. The mangers configure the
hosts extract the data stream from them.
"""
from __future__ import annotations

import asyncio
import logging
import re
from contextlib import AsyncExitStack
from typing import Any, Set
from uuid import UUID

import asyncio_mqtt
import simplejson as json
from aiostream import pipe, stream

from async_event_bus import TopicNotRegisteredError, event_bus
from data_types import DataEvent
from databases import CONTEXTS as DATABASE_CONTEXTS
from databases import MongoDb
from errors import UnknownDriverError
from helper_functions import cancel_all_tasks, catch, iterate_safely
from sensors.transports.transport_factory import transport_factory

EVENT_BUS_DATA = "sensor_data/all"
MQTT_DATA_TOPIC = "sensors/{driver}/{uid}/{sid}"


class MqttManager:
    """This manager will take the sensor data from the event_bus backend and publish them onto the MQTT network"""

    def __init__(self, node_id: UUID | None, host: str, port: int, number_of_workers: int = 5) -> None:
        self.__node_id = node_id
        self.__logger = logging.getLogger(__name__)
        self.__host = host
        self.__port = port
        self.__number_of_workers = number_of_workers

    async def producer(self, output_queue: asyncio.Queue[tuple[str, dict[str, str | float | int]]]) -> None:
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
                    "timestamp": event.timestamp,
                    "uuid": str(event.sender),
                    "sid": event.sid,
                    "value": event.value,
                    "unit": event.unit,
                }
            except Exception:  # pylint: disable=broad-except
                self.__logger.exception("Malformed data received. Dropping data: %s", event)
            else:
                output_queue.put_nowait((topic, payload))

    @staticmethod
    def _calculate_timeout(last_reconnect_attempt: float, reconnect_interval: float) -> float:
        """
        Calculates the time to wait between reconnect attempts.
        Parameters
        ----------
        last_reconnect_attempt: A timestamp in seconds
        reconnect_interval: The reconnect interval in seconds

        Returns
        -------
        float
            The number of seconds to wait. This is a number greater than 0.
        """
        return max(0.0, reconnect_interval - (asyncio.get_running_loop().time() - last_reconnect_attempt))

    def _log_mqtt_error_code(self, worker_name: str, error_code: int | None, previous_error_code: int) -> None:
        """
        Log the MQTT error codes as human-readable errors to the logger (error log level). If the code is unknown, log
        it as an exception for debugging. Suppresses errors, if they are repeated.
        Parameters
        ----------
        worker_name: str
            The name of the worker, prepended to the message
        error_code: int
            The current error code
        previous_error_code: int or None
            The error code of the previous error or None if there was none.
        """
        if error_code == previous_error_code:
            return  # Only log an error once
        if error_code == 111:
            self.__logger.error(
                "Worker (%s): Connection refused by MQTT server (%s:%i). Retrying.",
                worker_name,
                self.__host,
                self.__port,
            )
        elif error_code == 113:
            self.__logger.error(
                "Worker (%s): MQTT server (%s:%i) is unreachable. Retrying.",
                worker_name,
                self.__host,
                self.__port,
            )
        elif error_code == 7:
            self.__logger.error(
                "Worker (%s): The connection to MQTT server (%s:%i) was lost. Retrying.",
                worker_name,
                self.__host,
                self.__port,
            )
        elif error_code == -3:
            self.__logger.error(
                "Worker (%s): Temporary failure in name resolution of MQTT server (%s:%i). Retrying.",
                worker_name,
                self.__host,
                self.__port,
            )
        else:
            self.__logger.exception("Worker (%s): MQTT connection error (code: %i). Retrying.", worker_name, error_code)

    async def consumer(  # pylint: disable=too-many-branches
        self,
        input_queue: asyncio.Queue[tuple[str, dict[str, str | float | int]]],
        worker_name,
        reconnect_interval: int = 5,
    ) -> None:
        """
        Pushes the data from the input queue to the MQTT broker. It will make sure,
        that no data is lost if the MQTT broker disconnects.

        Parameters
        ----------
        input_queue: asyncio.Queue
            The queue, that supplies the worker with data
        worker_name: str
            A human-readable label used for logging
        reconnect_interval: int, default=5
            The time in seconds to wait between connection attempts.
        """
        error_code = 0  # 0 = success
        event = None
        last_reconnect_attempt = asyncio.get_running_loop().time() - reconnect_interval
        while "not connected":
            # Wait for at least reconnect_interval before connecting again
            timeout = self._calculate_timeout(last_reconnect_attempt, reconnect_interval)
            if round(timeout) > 0:
                # Do not print '0 s' as this is confusing.
                self.__logger.info(
                    "Worker (%s): Connecting to MQTT broker (%s:%i) in %.0f s due to rate limiting.",
                    worker_name,
                    timeout,
                    self.__host,
                    self.__port,
                )
            else:
                self.__logger.info(
                    "Worker (%s): Connecting to MQTT broker (%s:%i).", worker_name, self.__host, self.__port
                )
            await asyncio.sleep(timeout)
            last_reconnect_attempt = asyncio.get_running_loop().time()
            try:
                async with asyncio_mqtt.Client(
                    hostname=self.__host, port=self.__port, client_id=f"Labkraken-{self.__node_id}_worker-{worker_name}"
                ) as mqtt_client:
                    self.__logger.info(
                        "Worker (%s): Successfully connected to MQTT broker (%s:%i).",
                        worker_name,
                        self.__host,
                        self.__port,
                    )
                    while "loop not cancelled":
                        if event is None:
                            # only get new data if we have pushed everything to the broker
                            event = await input_queue.get()
                        try:
                            topic, payload = event
                            # convert payload to JSON
                            # Typically sensors return data as decimals or ints to preserve the precision
                            encoded_payload = json.dumps(payload, use_decimal=True)
                        except TypeError:
                            self.__logger.error(
                                "Worker (%s): Error while serializing DataEvent: %s. Dropping event.",
                                worker_name,
                                payload,
                            )
                        else:
                            # self.__logger.info("Going to publish: %s to %s", payload, topic)
                            await mqtt_client.publish(topic, payload=encoded_payload, qos=2)

                        # The event was either dropped or published
                        event = None  # Get a new event to publish
                        input_queue.task_done()
                        error_code = 0  # 0 = success
            except asyncio_mqtt.error.MqttCodeError as exc:
                self._log_mqtt_error_code(worker_name, error_code=exc.rc, previous_error_code=error_code)
                error_code = exc.rc
            except ConnectionRefusedError:
                self._log_mqtt_error_code(
                    worker_name, error_code=111, previous_error_code=error_code
                )  # Connection refused is code 111
                error_code = 111
            except asyncio_mqtt.error.MqttError as exc:
                error = re.search(r"^\[Errno (\d+)\]", str(exc))
                if error is not None:
                    self._log_mqtt_error_code(
                        worker_name, error_code=int(error.group(1)), previous_error_code=error_code
                    )
                    error_code = int(error.group(1))
                else:  # no match found
                    self.__logger.error(
                        "Worker (%s): Connection error of to MQTT broker (%s:%i). Retrying.",
                        worker_name,
                        self.__host,
                        self.__port,
                    )
            except Exception:  # pylint: disable=broad-except
                # Catch all exceptions, log them, then try to restart the worker.
                self.__logger.exception(
                    "Worker (%s): Error while publishing data to MQTT broker (%s:%i). Reconnecting.",
                    worker_name,
                    self.__host,
                    self.__port,
                )

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
        except Exception:  # pylint: disable=broad-except
            self.__logger.exception("Error during shutdown of the MQTT manager")

    async def run(self) -> None:
        """
        The main task, that spawns all workers.
        """
        async with AsyncExitStack() as stack:
            tasks: set[asyncio.Task] = set()
            stack.push_async_callback(self.cancel_tasks, tasks)
            event_queue: asyncio.Queue[tuple[str, dict]] = asyncio.Queue()

            consumers = {
                asyncio.create_task(self.consumer(event_queue, worker_name=f"MQTT consumer {i}"))
                for i in range(self.__number_of_workers)
            }
            tasks.update(consumers)

            task = asyncio.create_task(self.producer(event_queue))
            tasks.add(task)

            await asyncio.gather(*tasks)


class DatabaseManager:
    """This manager reads the configuration data from the database and publishes it on the event_bus network."""

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
        except Exception:  # pylint: disable=broad-except
            self.__logger.exception("Error during shutdown of the %s", type(self).__name__)

    async def run(self) -> None:
        """
        The main task, that spawn all workers.
        """
        while "loop not cancelled":
            # TODO: Notify all hosts/sensors, when the database is connected
            try:
                async with AsyncExitStack() as stack:
                    tasks: set[asyncio.Task] = set()
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
            except Exception:  # pylint: disable=broad-except
                # Catch all exceptions, log them, then try to restart the worker.
                self.__logger.exception("Error while processing database.")
                await asyncio.sleep(5)


class HostManager:  # pylint: disable=too-few-public-methods
    """This manager creates the sensor objects and reads data from them to publish it onto the event_bus."""

    def __init__(self, node_id: UUID | None) -> None:
        self.__node_id = node_id
        self.__topic = "db_autodiscovery_sensors"

    @staticmethod
    def _create_transport(config: dict[str, Any]):
        if config is None:
            return None
        try:
            return transport_factory.get(**config)
        except UnknownDriverError:
            logging.getLogger(__name__).warning("No driver available for transport '%s'.", config["driver"])
        except Exception:  # pylint: disable=broad-except
            # catch all exceptions here, because a faulty driver should not bring down the daemon
            logging.getLogger(__name__).exception("Error while creating transport '%s'.", config["driver"])
        return None

    @staticmethod
    def _is_config_valid(node_id: UUID | None, config: dict[str, Any]) -> bool:
        """
        A config is valid, if it is enabled and assigned to this logger node. The latter is the case if either this node
        has no uuid, the config has no node id (served by loggers) or the config matches this node id.
        Parameters
        ----------
        node_id: UUID
            The uuid of this logger node
        config: dict
            The configuration dictionary

        Returns
        -------
        bool
            True if the configuration is valid and served by this node.
        """
        return (
            config is not None
            and config["enabled"]
            and (config["node_id"] is None or node_id is None or config["node_id"] == node_id)
        )

    async def run(self) -> None:
        """
        The main task, that reads data from the sensors and pushes it onto the event_bus.
        """
        # Generate the UUIDs of new sensors
        sensor_stream = stream.chain(
            stream.iterate(iterate_safely(f"{self.__topic}/get", f"{self.__topic}/status_update")),
            stream.iterate(event_bus.subscribe(f"{self.__topic}/add_host")),
        ) | pipe.flatmap(
            lambda item: stream.chain(
                (stream.call(event_bus.call, f"{self.__topic}/get_config", item) | catch.pipe(TopicNotRegisteredError)),
                stream.iterate(event_bus.subscribe(f"nodes/by_uuid/{item}/update")),
            )
            | pipe.until(lambda config: config is None)
            | pipe.map(lambda config: config if self._is_config_valid(self.__node_id, config) else None)
            | pipe.map(self._create_transport)
            | pipe.switchmap(
                lambda transport: stream.empty() if transport is None else stream.iterate(transport.stream_data())
            )
            | pipe.action(lambda data: event_bus.publish("wamp/publish", data))
        )

        await sensor_stream
