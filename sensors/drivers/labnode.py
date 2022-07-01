"""
This is a wrapper for Labnode devices.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from aiostream import pipe, stream
from labnode_async.labnode import Labnode

from async_event_bus import event_bus
from data_types import DataEvent
from helper_functions import call_safely, create_device_function
from errors import ConfigurationError


class LabnodeSensor:
    """This class extends the driver with catch-all arguments in the constructor"""
    @classmethod
    @property
    def driver(cls) -> str:
        """
        Returns
        -------
        str
            The driver that identifies it to the sensor factory
        """
        return "labnode"

    def __init__(
            self,
            device: Labnode
    ) -> None:
        self._device = device
        self.__uuid: UUID | None = None
        self._logger = logging.getLogger(__name__)

    async def enumerate(self):
        self.__uuid = await self._device.get_uuid()

    def stream_data(self):
        # Generates the first configuration
        # Query the database and if it does not have a config for the sensor, wait until there is one

        data_stream = (
            stream.chain(
                stream.call(call_safely, "db_labnode_sensors/get_config",
                            "db_labnode_sensors/status_update", self.__uuid)
                | pipe.takewhile(lambda config: config is not None),
                stream.iterate(event_bus.subscribe(f"nodes/by_uuid/{self.__uuid}"))
            )
            | pipe.action(
                lambda config: logging.getLogger(__name__).info(
                    "Got new configuration for: %s", self._device,
                )
            )
            | pipe.map(self._create_config)
            | pipe.switchmap(self._configure_and_stream)
        )

        return data_stream

    def _create_config(self, config):
        if config is None:
            return None
        try:
            on_connect = tuple(
                create_device_function(self._device, func_call) for func_call in config['on_connect']
            )
            config['on_connect'] = on_connect
        except ConfigurationError:
            config = None

        return config

    def _read_sensor(self, sid: int, interval: float, unit: str, topic: str, timeout: float):
        return (
            stream.repeat(stream.call(self._device.get_by_function_id, sid), interval=interval)
            | pipe.concat(task_limit=1)
            | pipe.timeout(timeout)
            | pipe.map(
                lambda value: DataEvent(
                    sender=self.__uuid, topic=topic, value=value, sid=sid, unit=str(unit)
                )
            )
        )

    @staticmethod
    def _parse_configuration(sid: str, config: dict[str, Any]):
        return int(sid), config['interval'], config['unit'], config['topic'],  config['timeout']

    def _configure_and_stream(self, config):
        if config is None:
            return stream.empty()
        try:
            # Run all config steps in order (concat) and one at a time (task_limit=1). Drop the output. There is
            # nothing to compare them to (filter => false), then read all sensors of the bicklet and process them in
            # parallel (flatten).
            config_stream = (
                stream.chain(
                    stream.iterate(config['on_connect'])
                    | pipe.starmap(lambda func, timeout: stream.just(func()) | pipe.timeout(timeout))
                    | pipe.concat(task_limit=1)
                    | pipe.filter(lambda result: False),
                    stream.iterate(config['config'].items())
                    | pipe.starmap(self._parse_configuration)
                    | pipe.starmap(self._read_sensor)
                    | pipe.flatten()
                )
            )
            return config_stream
        except Exception:
            self._logger.exception("This should not happen")
            raise
