"""
This is a wrapper for the Prologix Ethernet controller used by other generic devices like the SCPI driver. It wraps
the Prologix library and adds the stream interface via GenericTransport.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from aiostream import async_, pipe
from labnode_async import IPConnection as LabnodeIPConnection

from helper_functions import with_context
from sensors.drivers.labnode import LabnodeSensor
from sensors.transports.generic_ethernet_transport import GenericEthernetTransport


class LabnodeTransport(GenericEthernetTransport, LabnodeIPConnection):
    """
    The transport wrapper for the generic ethernet connection
    """

    @classmethod
    def driver(cls) -> str:
        """
        Returns
        -------
        str
            The driver name that identifies it to the transport factory
        """
        return "labnode"

    @property
    def uri(self) -> str:
        """
        Returns
        -------
        str
            A string representation of the connection.
        """
        return f"{self.hostname}:{self.port}"

    def __init__(  # pylint: disable=too-many-arguments  # The parameters are coming from a (relational) database
        self,
        hostname: str,
        port: int,
        reconnect_interval: float | None,
        uuid: UUID,
        label: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> None:
        super().__init__(
            uuid=uuid,
            database_topic="db_labnode",
            transport_name="APQ LabNode",
            reconnect_interval=reconnect_interval,
            label=label,
            hostname=hostname,
            port=port,
        )

    def _stream_data(self, transport):
        config_stream = (
            with_context(
                transport,
                on_exit=lambda: logging.getLogger(__name__).info(
                    "Disconnected from APQ LabNode at %s (%s).", transport.uri, transport.label
                ),
            )
            | pipe.action(
                lambda _: logging.getLogger(__name__).info(
                    "Connected to APQ LabNode at %s (%s).", transport.uri, transport.label
                )
            )
            | pipe.map(LabnodeSensor)
            | pipe.action(async_(lambda sensor: sensor.enumerate()))
            | pipe.switchmap(lambda sensor: sensor.stream_data())
        )

        return config_stream
