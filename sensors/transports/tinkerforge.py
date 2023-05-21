"""
This is a asyncIO driver for a generic SCPI compatible device.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from aiostream import pipe, stream
from tinkerforge_async import IPConnectionAsync
from tinkerforge_async.devices import Device
from tinkerforge_async.ip_connection import EnumerationType

from async_event_bus import TopicNotRegisteredError, event_bus
from helper_functions import context, retry
from sensors.drivers.tinkerforge import TinkerforgeSensor


class TinkerforgeTransport(IPConnectionAsync):
    """
    The transport wrapper for the tinkerforge ethernet connection
    """

    @classmethod
    def driver(cls) -> str:
        """
        Returns
        -------
        str
            The driver name that identifies it to the transport factory
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
        self, hostname: str, port: int, reconnect_interval: float | None, label: str, *_args: Any, **_kwargs: Any
    ) -> None:
        super().__init__(hostname, port)
        self.__label = label
        self.__reconnect_interval = 1 if reconnect_interval is None else reconnect_interval

    @staticmethod
    async def filter_enumerations(enumeration: tuple[EnumerationType, Device]) -> bool:
        """
        Filter enumerations. If the device exists, it returns False.
        Parameters
        ----------
        enumeration tuple of EnumerationType and Device

        Returns
        -------
        bool
            Returns True if a new device should be created
        """
        enumeration_type, device = enumeration
        if enumeration_type is EnumerationType.DISCONNECTED:
            return False
        if enumeration_type is EnumerationType.AVAILABLE:
            try:
                await event_bus.call(f"nodes/tinkerforge/{device.uid}/status")
            except TopicNotRegisteredError:
                return True  # The device does not exist, so we need to create one
            return False  # The device exists, there is no need to create it
        if enumeration_type is EnumerationType.CONNECTED:
            return True
        assert False, "unreachable"

    @staticmethod
    async def notify_device(enumeration: tuple[EnumerationType, Device]) -> None:
        """
        Filter enumerations. If the device exists, it returns False.
        Parameters
        ----------
        enumeration tuple of EnumerationType and Device

        Returns
        -------
        bool
            Returns True if a new device should be created
        """
        enumeration_type, device = enumeration
        if enumeration_type is EnumerationType.DISCONNECTED:
            event_bus.publish(f"nodes/tinkerforge/{device.uid}/remove", None)

    @staticmethod
    def _stream_transport(transport: TinkerforgeTransport):
        sensor_stream = stream.chain(
            stream.call(transport.enumerate) | pipe.filter(lambda x: False),  # drop the result, because we don't care
            stream.iterate(transport.read_enumeration())
            | pipe.action(transport.notify_device)  # Notify clients to shut down if necessary
            | pipe.filter(transport.filter_enumerations)  # Only proceed with if the client is not yet created
            | pipe.starmap(lambda enumeration_type, sensor: TinkerforgeSensor(sensor))
            | pipe.map(lambda sensor: sensor.stream_data())
            | pipe.flatten(),
        ) | context.pipe(
            transport,
            on_enter=lambda: logging.getLogger(__name__).info(
                "Connected to Tinkerforge host at %s (%s).", transport.uri, transport.label
            ),
            on_exit=lambda exit_code: logging.getLogger(__name__).info(
                "Disconnected from Tinkerforge host at %s (%s). Reason: %s.", transport.uri, transport.label, exit_code
            ),
        )
        return sensor_stream

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
