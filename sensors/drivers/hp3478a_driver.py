"""
This is an asyncIO driver for a generic SCPI compatible device.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from hp3478a_async import HP_3478A

from sensors.drivers.generic_driver import GenericDriver

if TYPE_CHECKING:
    from sensors.transports.ethernet import EthernetTransport
    from sensors.transports.prologix_ethernet import PrologixEthernetTransport


class Hp3478ADriver(GenericDriver, HP_3478A):
    """This class extends the HP 3478A driver with catch-all arguments in the constructor"""
    @classmethod
    @property
    def driver(cls) -> str:
        """
        Returns
        -------
        str
            The driver that identifies it to the host factory
        """
        return "hp3478a2"

    def __init__(
            self,
            uuid,
            connection: EthernetTransport | PrologixEthernetTransport,
            *_args,
            **_kwargs
    ) -> None:
        super().__init__(uuid, connection)
