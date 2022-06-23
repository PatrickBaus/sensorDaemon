"""
This is a generic transport driver implementing the basic streams used by transports, that cannot enumerate its devices.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import UUID

try:
    from typing import Self  # Python >=3.11
except ImportError:
    from typing_extensions import Self

from aiostream import pipe, stream

from async_event_bus import event_bus
from errors import UnknownDriverError
from helper_functions import call_safely, retry
from sensors.factories.sensor_factory import sensor_factory


class GenericTransport:
    """
    The transport base class for a generic connection
    """
    @property
    def reconnect_interval(self) -> float:
        """
        Returns
        -------
        float
            The reconnect interval in seconds
        """
        return self.__reconnect_interval

    @property
    def uuid(self) -> UUID:
        """
        Returns
        -------
        UUID
            The unique identifier of the transport
        """
        return self.__uuid

    def __init__(
            self,
            uuid: UUID,
            database_topic: str,
            label: str,
            reconnect_interval: float | None,
            *args: Any,
            **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.__uuid = uuid
        self.__label = label
        self.__database_topic = database_topic
        self.__reconnect_interval = 1 if reconnect_interval is None else reconnect_interval
        self.__logger = logging.getLogger(__name__)

    @staticmethod
    def _create_device(transport: Any, config: dict[str, Any]):
        if config is None:
            return None, None
        try:
            return config, sensor_factory.get(connection=transport, **config)
        except UnknownDriverError:
            logging.getLogger(__name__).warning(f"No driver available for device '{config['driver']}'")
        except Exception:
            logging.getLogger(__name__).exception("Error while creating device '{config['driver']}'")
        return None, None

    async def _stream_transport(self, transport):
        async with transport:
            try:
                logging.getLogger(__name__).info(
                    "Connected to %s at %s.", self.__label, transport.uri
                )
                config_stream = (
                    stream.chain(
                        stream.call(
                            call_safely,
                            f"{self.__database_topic}/get_config",
                            f"{self.__database_topic}/status_update",
                            transport.uuid
                        ),
                        stream.iterate(event_bus.subscribe(f"nodes/by_uuid/{transport.uuid}/add"))
                    )
                    | pipe.map(lambda config: self._create_device(transport, config))
                    | pipe.starmap(
                        lambda config, device: stream.empty() if device is None else device.stream_data(config)
                    )
                    | pipe.switch()
                )

                async with config_stream.stream() as streamer:
                    async for item in streamer:
                        yield item
            finally:
                logging.getLogger(__name__).info(
                    "Disconnected from %s at %s.", self.__label, transport.uri
                )

    def stream_data(self):
        """
        Discover all Tinkerforge devices connected via this transport.
        Yields
        -------

        """
        data_stream = (
            stream.just(self)
            | pipe.action(
                lambda transport: self.__logger.info(
                    "Connecting to %s at %s.", self.__label, transport.uri
                )
            )
            | pipe.switchmap(self._stream_transport)
            | retry.pipe((OSError, asyncio.TimeoutError), self.reconnect_interval)
        )
        # We need to catch OSError, which is the parent of ConnectionError, because a connection to localhost
        # might resolve to 2 IPs and then return  multiple exception at once if all IPs fail, which is an
        # OSError.
        # We also need to catch the TimeoutError here, because most protocols like SCPI have no means of synchronizing
        # messages. This means, that we will loos sync after a timeout. We therefore need to reconnect in these cases.

        return data_stream
