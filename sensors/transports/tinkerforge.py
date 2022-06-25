"""
This is a asyncIO driver for a generic SCPI compatible device.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from aiostream import pipe, stream
from tinkerforge_async import IPConnectionAsync
from tinkerforge_async.ip_connection import EnumerationType, NotConnectedError

from async_event_bus import event_bus
from helper_functions import retry
from sensors.drivers.tinkerforge import TinkerforgeSensor


class TinkerforgeTransport(IPConnectionAsync):
    """
    The transport wrapper for the tinkerforge ethernet connection
    """
    @classmethod
    @property
    def driver(cls) -> str:
        """
        Returns
        -------
        str
            The driver that identifies it to the host factory
        """
        return "tinkerforge2"

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
    def uri(self) -> str:
        """
        Returns
        -------
        str
            A string representation of the connection.
        """
        return f"{self.hostname}:{self.port}"

    @property
    def label(self) -> str:
        """
        Returns
        -------
        str
            A label as a human-readable descriptor of the transport.
        """
        return self.__label

    def __init__(
            self,
            hostname: str,
            port: int,
            reconnect_interval: float | None,
            label: str,
            *_args: Any,
            **_kwargs: Any
    ) -> None:
        super().__init__(hostname, port)
        self.__label = label
        self.__reconnect_interval = 1 if reconnect_interval is None else reconnect_interval
        self.__logger = logging.getLogger(__name__)

    @staticmethod
    async def _stream_transport(transport: TinkerforgeTransport):
        async with transport:
            try:
                logging.getLogger(__name__).info(
                    "Connected to Tinkerforge host at %s (%s).", transport.uri, transport.label
                )
                sensor_stream = (
                    stream.chain(
                        stream.just(transport.enumerate()) | pipe.filter(lambda x: False),
                        stream.iterate(transport.read_enumeration())
                            | pipe.action(lambda enumeration: event_bus.publish(
                                    f"nodes/tinkerforge/{enumeration[1].uid}/remove", None)
                            )
                            | pipe.filter(lambda enumeration: enumeration[0] is not EnumerationType.DISCONNECTED)
                            | pipe.starmap(lambda enumeration_type, sensor: TinkerforgeSensor(sensor))
                            | pipe.map(lambda sensor: sensor.stream_data())
                            | pipe.flatten()
                    )
                )

                async with sensor_stream.stream() as streamer:
                    async for item in streamer:
                        yield item
            finally:
                logging.getLogger(__name__).info(
                    "Disconnected from Tinkerforge host at %s (%s).", transport.uri, transport.label
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
                lambda transport: logging.getLogger(__name__).info(
                    "Connecting to Tinkerforge host at %s (%s).", transport.uri, transport.label
                )
            )
            | pipe.switchmap(self._stream_transport)
            | retry.pipe((ConnectionError, asyncio.TimeoutError), self.reconnect_interval)
        )
        return data_stream
