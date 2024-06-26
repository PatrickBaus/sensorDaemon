"""
This is a wrapper for the Prologix Ethernet controller used by other generic devices like the SCPI driver. It wraps
the Prologix library and adds the stream interface via GenericTransport.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from prologix_gpib_async import AsyncPrologixGpibEthernetController

from helper_functions import retry
from sensors.transports.generic_ethernet_transport import GenericEthernetTransport


class PrologixEthernetTransport(GenericEthernetTransport, AsyncPrologixGpibEthernetController):
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
        return "prologix_gpib2"

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
            transport_name="Prologix GPIB controller",
            reconnect_interval=reconnect_interval,
            label=label,
            hostname=hostname,
            port=port,
            pad=pad,
            sad=sad,
        )

    def _stream_data(self, transport):
        return super()._stream_data(transport) | retry.pipe((ValueError,), self.reconnect_interval)
