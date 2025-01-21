"""
This file contains the implementation of the managers for the sensor hosts. All
hosts of a certain type are managed by their managers. The mangers configure the
hosts extract the data stream from them.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import re
from contextlib import AsyncExitStack
from typing import Any, Unpack
from uuid import UUID

import aiomqtt
import simplejson as json
from aiostream import pipe, stream
from pydantic import BaseModel, field_validator

from async_event_bus import TopicNotRegisteredError, event_bus
from data_types import DataEvent
from databases import CONTEXTS as DATABASE_CONTEXTS
from databases import DatabaseParams, MongoDb
from errors import UnknownDriverError
from helper_functions import cancel_all_tasks, catch, iterate_safely
from sensors.transports.transport_factory import transport_factory

EVENT_BUS_DATA = "sensor_data/all"
MQTT_DATA_TOPIC = "sensors/{driver}/{uid}/{sid}"


# A regular expression to match a hostname with an optional port.
# It adheres to RFC 1035 (https://www.rfc-editor.org/rfc/rfc1035) and matches ports
# between 0-65535.
HOSTNAME_REGEX = (
    r"^((?=.{1,255}$)[0-9A-Za-z](?:(?:[0-9A-Za-z]|-){0,61}[0-9A-Za-z])?(?:\.[0-9A-Za-z](?:(?:["
    r"0-9A-Za-z]|-){0,61}[0-9A-Za-z])?)*\.?)(?:\:([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{"
    r"2}|655[0-2][0-9]|6553[0-5]))?$"
)


class MQTTParams(BaseModel):
    """
    Parameters used to connect to the MQTT broker.

    Parameters
    ----------
    hosts: List of Tuple of str and int
        A list of host:port tuples. The list contains the servers of a cluster. If no port is provided it defaults to
        1883. If port number 0 is provided the default value of 1883 is used.
    identifier: str or None
        An MQTT client id used to uniquely identify a client to persist messages.
    username: str or None
        The username used for authentication. Set to None if no username is required
    password: str or None
        The password used for authentication. Set to None if no username is required
    """

    hosts: list[tuple[str, int]]
    identifier: str | None
    username: str | None
    password: str | None

    @field_validator("hosts", mode="before")
    @classmethod
    def ensure_list_of_hosts(cls, value: str) -> list[tuple[str, int]]:
        """
        Parse
        Parameters
        ----------
        value: str
            Either a single hostname:port string or a comma separated list of hostname:port strings.

        Returns
        -------
        list of tuple of str and int
            A list of (hostname, port) tuples.
        """
        hosts = value.split(",")
        result = []
        for host in hosts:
            host = host.strip()
            match = re.search(HOSTNAME_REGEX, host)
            if match is None:
                raise ValueError(f"'{value}' is not a valid hostname or list of hostnames.")
            result.append(
                (
                    match.group(1),
                    int(match.group(2)) if match.group(2) and not match.group(2) == "0" else 1883,
                )
            )
        return result


class MqttManager:
    """This manager will take the sensor data from the event_bus backend and publish them onto the MQTT network"""

    def __init__(
        self,
        node_id: UUID | None,
        broker_params: MQTTParams,
        number_of_workers: int = 5,
    ) -> None:
        self.__node_id = node_id
        self.__logger = logging.getLogger(__name__)
        self.__broker = broker_params
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

    def _log_mqtt_error_code(
        self, worker_name: str, host: tuple[str, int], error_code: str | int, previous_error_code: str | int
    ) -> None:
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
                "Worker (%s): Connection refused by MQTT broker (%s:%i). Retrying.",
                worker_name,
                *host,
            )
        elif error_code == 113:
            self.__logger.error("Worker (%s): MQTT broker (%s:%i) is unreachable. Retrying.", worker_name, *host)
        elif error_code == 7:
            self.__logger.error(
                "Worker (%s): The connection to MQTT broker (%s:%i) was lost. Retrying.", worker_name, *host
            )
        elif error_code == -2:
            self.__logger.error(
                "Worker (%s): Failure in name resolution of MQTT broker (%s:%i). Retrying.", worker_name, *host
            )
        elif error_code == -3:
            self.__logger.error(
                "Worker (%s): Temporary failure in name resolution of MQTT broker (%s:%i). Retrying.",
                worker_name,
                *host,
            )
        elif error_code == -5:
            self.__logger.error("Worker (%s): Unknown host name of MQTT broker (%s:%i). Retrying.", worker_name, *host)
        elif error_code == "timed out":
            self.__logger.error(
                "Worker (%s): The connection to MQTT broker (%s:%i) timed out. Retrying.", worker_name, *host
            )
        else:
            self.__logger.exception("Worker (%s): MQTT connection error (code: %s). Retrying.", worker_name, error_code)

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
        error_code: str | int = 0  # 0 = success
        event: tuple[str, dict[str, str | float | int]] | None = None
        previous_reconnect_attempt = asyncio.get_running_loop().time() - reconnect_interval
        for host in itertools.cycle(self.__broker.hosts):  # iterate over the list of hostnames until the end of time
            # Wait for at least reconnect_interval before connecting again
            timeout = self._calculate_timeout(previous_reconnect_attempt, reconnect_interval)
            if round(timeout) > 0:
                # Do not print '0 s' as this is confusing.
                self.__logger.info(
                    "Worker (%s): Connecting to MQTT broker (%s:%i) in %.0f s due to rate limiting.",
                    worker_name,
                    *host,
                    timeout,
                )
            else:
                self.__logger.info("Worker (%s): Connecting to MQTT broker (%s:%i).", worker_name, *host)
            await asyncio.sleep(timeout)
            previous_reconnect_attempt = asyncio.get_running_loop().time()
            try:
                async with aiomqtt.Client(
                    identifier=f"Labkraken-{self.__node_id}_worker-{worker_name}",
                    hostname=host[0],
                    port=host[1],
                    **self.__broker.model_dump(exclude={"hosts"}),
                ) as mqtt_client:
                    self.__logger.info(
                        "Worker (%s): Successfully connected to MQTT broker (%s:%i).", worker_name, *host
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
            except aiomqtt.MqttCodeError as exc:
                self._log_mqtt_error_code(worker_name, host=host, error_code=exc.rc, previous_error_code=error_code)
                error_code = exc.rc
            except ConnectionRefusedError:
                self._log_mqtt_error_code(
                    worker_name, host=host, error_code=111, previous_error_code=error_code
                )  # Connection refused is code 111
                error_code = 111
            except aiomqtt.MqttError as exc:
                error = re.search(r"\[Errno ([+-]?\d+)]", str(exc))
                if error is not None:
                    self._log_mqtt_error_code(
                        worker_name,
                        host=host,
                        error_code=int(error.group(1)),
                        previous_error_code=error_code,
                    )
                    error_code = int(error.group(1))
                else:  # no match found
                    self._log_mqtt_error_code(
                        worker_name, host=host, error_code=str(exc), previous_error_code=error_code
                    )
                    error_code = str(exc)
            except Exception:  # pylint: disable=broad-except
                # Catch all exceptions, log them, then try to restart the worker.
                self.__logger.exception(
                    "Worker (%s): Error while publishing data to MQTT broker (%s:%i). Reconnecting.", worker_name, *host
                )

    async def cancel_tasks(self, tasks: set[asyncio.Task]) -> None:
        """
        Cancel all tasks and log any exceptions raised.

        Parameters
        ----------
        tasks: set[asyncio.Task]
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

    def __init__(self, **kwargs: Unpack[DatabaseParams]) -> None:
        self.__logger = logging.getLogger(__name__)
        self.__database_driver = MongoDb(**kwargs)

    async def cancel_tasks(self, tasks: set[asyncio.Task]) -> None:
        """
        Cancel all tasks and log any exceptions raised.

        Parameters
        ----------
        tasks: set[asyncio.Task]
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

                    await stack.enter_async_context(self.__database_driver)
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
