"""
This file contains a factory to select the correct driver for all supported
sensor hosts.
"""
from typing import Any, Type

from errors import UnknownDriverError
from .ethernet import EthernetTransport
from .linux_gpib import LinuxGpibTransport
from .prologix_ethernet import PrologixEthernetTransport
from .tinkerforge import TinkerforgeTransport


class TransportFactory:
    """
    A transport factory to select the correct driver for given database config.
    """
    def __init__(self):
        self.__available_drivers: dict[str, Type[Any]] = {}

    def register(self, transport: Any) -> None:
        """
        Register a driver with the factory. Should only be called in this file.

        Parameters
        ----------
        transport: Any
            The transport driver to register.
        """
        self.__available_drivers[transport.driver] = transport

    # TODO: Type hinting
    def get(self, driver: str, **kwargs: Any):  # pylint: disable=too-many-arguments
        """
        Look up the driver for a given database entry. Raises a `ValueError` if
        the driver is not registered.

        Parameters
        ----------
        driver: str
            A string identifying the driver.
        **kwargs : tuple
            Additional keyword arguments that will be passed to the constructor of the class produced
        Returns
        -------
        Any
            A registered sensor host

        Raises
        ----------
        ValueError
        """
        host = self.__available_drivers.get(driver)
        if host is None:
            raise UnknownDriverError(f"No driver available for transport '{driver}'")
        return host(**kwargs)


transport_factory = TransportFactory()
transport_factory.register(TinkerforgeTransport)
transport_factory.register(PrologixEthernetTransport)
# host_factory.register(LabnodeSensorHost)
transport_factory.register(EthernetTransport)
transport_factory.register(LinuxGpibTransport)
