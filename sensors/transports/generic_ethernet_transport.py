"""
This is a generic transport driver implementing error handling for an ethernet streamer.
"""
from __future__ import annotations

import asyncio
import logging

from aiostream import pipe, stream

from helper_functions import retry
from sensors.transports.generic_transport import GenericTransport


class GenericEthernetTransport(GenericTransport):
    """
    The transport base class for a generic ethernet connection.
    """

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
            | retry.pipe((OSError, asyncio.TimeoutError), self.reconnect_interval)
        )
        # We need to catch OSError, which is the parent of ConnectionError, because a connection to localhost
        # might resolve to 2 IPs and then return  multiple exception at once if all IPs fail, which is an
        # OSError.
        # We also need to catch the TimeoutError here, because most protocols like SCPI have no means of synchronizing
        # messages. This means, that we will lose sync after a timeout. We therefore need to reconnect in these cases.

        return data_stream
