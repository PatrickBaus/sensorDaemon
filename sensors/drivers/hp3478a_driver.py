"""
This is an asyncIO driver for a generic SCPI compatible device.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hp3478a_async import HP_3478A

from sensors.drivers.generic_driver import GenericDriverMixin

if TYPE_CHECKING:
    from sensors.transports.ethernet import EthernetTransport
    from sensors.transports.prologix_ethernet import PrologixEthernetTransport


class Hp3478ADriver(GenericDriverMixin, HP_3478A):
    """This class extends the HP 3478A driver with catch-all arguments in the constructor"""

    @classmethod
    def driver(cls) -> str:
        """
        Returns
        -------
        str
            The driver name that identifies it to the sensor factory
        """
        return "hp3478a2"

    def __init__(
        self, uuid, connection: EthernetTransport | PrologixEthernetTransport, *_args: Any, **_kwargs: Any
    ) -> None:
        super().__init__(uuid, connection)
