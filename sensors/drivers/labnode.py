"""
This is a wrapper for LabNode devices.
"""

# pylint: disable=duplicate-code
from __future__ import annotations

import logging
from typing import Any, AsyncGenerator
from uuid import UUID

from aiostream import pipe, stream
from labnode_async.labnode import Labnode

from async_event_bus import event_bus
from data_types import DataEvent
from errors import ConfigurationError, SensorNotReady
from helper_functions import call_safely, create_device_function


class LabnodeSensor:
    """This class extends the driver with catch-all arguments in the constructor"""

    @classmethod
    def driver(cls) -> str:
        """
        Returns
        -------
        str
            The driver name that identifies it to the sensor factory
        """
        return "labnode"

    def __init__(self, device: Labnode) -> None:
        self._device = device
        self.__uuid: UUID | None = None
        self._logger = logging.getLogger(__name__)

    async def enumerate(self) -> None:
        """Query the device for its UUID. This function must be called before streaming data."""
        self.__uuid = await self._device.get_uuid()

    def stream_data(self) -> AsyncGenerator[DataEvent, None]:
        """
        Generate the initial configuration of the sensor, configure it, and finally stream the data from the sensor.
        If there is a configuration update, reconfigure the sensor and start streaming again.
        Returns
        -------
        AsyncGenerator of DataEvent
            The data from the device
        """
        # Generates the first configuration
        # Query the database and if it does not have a config for the sensor, wait until there is one

        data_stream = (
            stream.chain(
                stream.call(
                    call_safely, "db_labnode_sensors/get_config", "db_labnode_sensors/status_update", self.__uuid
                )
                | pipe.takewhile(lambda config: config is not None),
                stream.merge(
                    *[
                        stream.iterate(event_bus.subscribe(f"nodes/by_uuid/{self.__uuid}/{action}"))
                        for action in ("add", "update")
                    ]
                ),
            )
            | pipe.action(
                lambda config: logging.getLogger(__name__).info(
                    "Got new configuration for: %s",
                    self._device,
                )
            )
            | pipe.map(self._create_config)
            | pipe.switchmap(
                lambda config: (
                    stream.empty() if config is None or not config["enabled"] else (self._configure_and_stream(config))
                )
            )
        )

        return data_stream

    def _create_config(self, config: dict[str, Any] | None) -> dict[str, Any] | None:
        if config is None:
            return None
        try:
            on_connect = tuple(create_device_function(self._device, func_call) for func_call in config["on_connect"])
            config["on_connect"] = on_connect
        except ConfigurationError:
            self._logger.error("Invalid configuration for %s: config=%s", self._device, config["on_connect"])
            config = None

        return config

    def _read_sensor(  # pylint: disable=too-many-arguments
        self, sid: int, interval: float, unit: str, topic: str, timeout: float
    ) -> AsyncGenerator[DataEvent, None]:
        if self.__uuid is None:
            raise SensorNotReady("You must enumerate the sensor before reading.")
        return (
            stream.repeat(stream.call(self._device.get_by_function_id, sid), interval=interval)
            | pipe.concat(task_limit=1)
            | pipe.timeout(timeout)
            | pipe.map(lambda value: DataEvent(sender=self.__uuid, topic=topic, value=value, sid=sid, unit=str(unit)))
        )

    @staticmethod
    def _parse_configuration(sid: str, config: dict[str, Any]):
        return int(sid), config["interval"], config["unit"], config["topic"], config["timeout"]

    def _configure_and_stream(self, config: dict[str, Any] | None) -> AsyncGenerator[DataEvent, None]:
        if config is None:
            return stream.empty()
        try:
            # Run all config steps in order (concat) and one at a time (task_limit=1). Drop the output. There is
            # nothing to compare them to (filter => false), then read all sensors of the node and process them in
            # parallel (flatten).
            config_stream = stream.chain(
                stream.iterate(config["on_connect"])
                | pipe.starmap(lambda func, timeout: stream.just(func()) | pipe.timeout(timeout))
                | pipe.concat(task_limit=1)
                | pipe.filter(lambda result: False),
                stream.iterate(config["config"].items())
                | pipe.starmap(self._parse_configuration)
                | pipe.starmap(self._read_sensor)
                | pipe.flatten(),
            )
            return config_stream
        except Exception:
            self._logger.exception("This should not happen.")
            raise
