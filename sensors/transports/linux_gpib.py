"""
This is a wrapper for the Prologix Ethernet controller used by other generic devices like the SCPI driver. It wraps
the Prologix library and adds the stream interface via GenericTransport.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import UUID

from aiostream import pipe, stream
from async_gpib import AsyncGpib, GpibError

from helper_functions import catch, retry
from sensors.transports.generic_transport import GenericTransport


class LinuxGpibTransport(GenericTransport, AsyncGpib):
    """
    The transport wrapper for the generic ethernet connection
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
        return "linux-gpib"

    @property
    def uri(self) -> str:
        """
        Returns
        -------
        str
            A string representation of the connection.
        """
        return str(self.id)

    def __init__(
        self,
        hostname: int | str,
        pad: int,
        sad: int,
        reconnect_interval: float | None,
        uuid: UUID,
        label: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> None:
        super().__init__(
            uuid=uuid,
            database_topic="db_generic_sensors",
            transport_name="Linux-GPIB controller",
            reconnect_interval=reconnect_interval,
            label=label,
            name=hostname,
            pad=pad,
            sad=sad,
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
                    "Connecting to %s at %s (%s).", transport.name, transport.uri, transport.label
                )
            )
            | pipe.switchmap(self._stream_data)
            | retry.pipe((GpibError, asyncio.TimeoutError), self.reconnect_interval)
        )
        # We need to catch the TimeoutError here, because most protocols like SCPI have no means of synchronizing
        # messages. This means, that we will lose sync after a timeout. In these cases, we reconnect the transport.
        # In case of a GpibError, we error out and stop the device.

        return data_stream
