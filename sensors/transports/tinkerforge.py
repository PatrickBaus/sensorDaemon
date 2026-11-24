"""
This is a asyncIO driver for a generic SCPI compatible device.
"""

from __future__ import annotations

import asyncio
import logging
from functools import partial
from typing import Any, AsyncGenerator

from aiostream import Stream, pipe, stream
from tinkerforge_async import IPConnectionAsync
from tinkerforge_async.devices import Device
from tinkerforge_async.ip_connection import EnumerationType

from async_event_bus import event_bus
from helper_functions import context, describe_exit, retry
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
    async def notify_device(enumeration: tuple[EnumerationType, Device]) -> None:
        """
        Call the "nodes/tinkerforge/{device.uid}/remove" event bus topic if the
        enumeration type is DISCONNECTED.

        Parameters
        ----------
        enumeration tuple of EnumerationType and Device
        """
        enumeration_type, device = enumeration
        if enumeration_type is EnumerationType.DISCONNECTED:
            event_bus.publish(f"nodes/tinkerforge/{device.uid}/remove", None)

    @staticmethod
    async def _iterate_queue(queue: asyncio.Queue[Device]) -> AsyncGenerator[Device, None]:
        """Yield the items put into the queue forever."""
        while True:
            yield await queue.get()

    @classmethod
    def route_enumeration(
        cls, device_queues: dict[int, asyncio.Queue[Device]], enumeration: tuple[EnumerationType, Device]
    ) -> Stream[Any] | None:
        """
        Route an enumeration to the sensor of the device. There is only one sensor per uid. The first enumeration of a
        device creates a sensor stream for that uid. Subsequent CONNECTED enumerations, e.g. sent after a bricklet was
        reset or reconnected, replace the sensor of that uid. The old sensor is shut down (and its event bus topics are
        unregistered) before the new one is started, so that it is configured again.

        Parameters
        ----------
        device_queues: dict of int and asyncio.Queue
            The queues of the devices already known on this connection, indexed by their uid
        enumeration: tuple of EnumerationType and Device
            The enumeration received from the host

        Returns
        -------
        Stream or None
            The data stream of a new device, or None if the device is already known or was disconnected
        """
        enumeration_type, device = enumeration
        if enumeration_type is EnumerationType.DISCONNECTED:
            # The sensor is notified via the event bus by notify_device() and shuts down.
            return None
        queue = device_queues.get(device.uid)
        if queue is not None:
            if enumeration_type is EnumerationType.CONNECTED:
                # The device was reset or reconnected, so it needs to be configured again
                queue.put_nowait(device)
            return None
        queue = asyncio.Queue()
        device_queues[device.uid] = queue
        queue.put_nowait(device)
        return stream.iterate(cls._iterate_queue(queue)) | pipe.switchmap(
            lambda new_device: TinkerforgeSensor(new_device).stream_data()
        )

    @staticmethod
    def _stream_transport(transport: TinkerforgeTransport):
        sensor_stream = stream.chain(
            stream.call(transport.enumerate) | pipe.filter(lambda x: False),  # drop the result, because we don't care
            stream.iterate(transport.read_enumeration())
            | pipe.action(transport.notify_device)  # Notify clients to shut down if necessary
            | pipe.map(partial(transport.route_enumeration, {}))  # The device queues are local to this connection
            | pipe.filter(lambda sensor_stream: sensor_stream is not None)
            | pipe.flatten(),
        ) | context.pipe(
            transport,
            on_enter=lambda: logging.getLogger(__name__).info(
                "Connected to Tinkerforge host at %s (%s).", transport.uri, transport.label
            ),
            on_exit=lambda exit_code: logging.getLogger(__name__).info(
                "Disconnected from Tinkerforge host at %s (%s). Reason: %s.",
                transport.uri,
                transport.label,
                describe_exit(exit_code),
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
            | retry.pipe((OSError, asyncio.TimeoutError), self.reconnect_interval)
        )
        return data_stream
