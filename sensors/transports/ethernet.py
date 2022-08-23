"""
This is a wrapper for a generic Ethernet transport used by other generic devices like the SCPI driver. It wraps
the IP connection and adds the stream interface via GenericTransport.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from connections import GenericIpConnection
from sensors.transports.generic_ethernet_transport import GenericEthernetTransport


class EthernetTransport(GenericEthernetTransport, GenericIpConnection):
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
        return "generic_ethernet2"

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
        uuid: UUID,
        hostname: str,
        port: int,
        reconnect_interval: float | None,
        label: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> None:
        super().__init__(
            uuid=uuid,
            database_topic="db_generic_sensors",
            transport_name="Ethernet host",
            reconnect_interval=reconnect_interval,
            label=label,
            hostname=hostname,
            port=port,
        )
