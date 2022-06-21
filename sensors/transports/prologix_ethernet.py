"""
This is a wrapper for the Prologix Ethernet controller used by other generic devices like the SCPI driver. It wraps
the Prologix library and adds the stream interface via GenericTransport.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from prologix_gpib_async import AsyncPrologixGpibEthernetController

from sensors.transports.generic_transport import GenericTransport


class PrologixEthernetTransport(GenericTransport, AsyncPrologixGpibEthernetController):
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

    def __init__(
            self,
            hostname: str,
            port: int,
            pad: int,
            sad: int,
            reconnect_interval: float | None,
            uuid: UUID,
            *_args: Any,
            **_kwargs: Any
    ) -> None:
        super().__init__(
            uuid=uuid,
            database_topic="db_generic_sensors",
            label="Prologix GPIB controller",
            reconnect_interval=reconnect_interval,
            hostname=hostname,
            port=port,
            pad=pad,
            sad=sad
        )
