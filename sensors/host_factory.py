# -*- coding: utf-8 -*-
"""
This file contains a factory to select the correct driver for all supported
sensor hosts.
"""
from .tinkerforge_host import TinkerforgeSensorHost


class SensorHostFactory:
    """
    A senor host factory to select the correct driver for given database
    config.
    """
    def __init__(self):
        self.__available_hosts = {}

    def register(self, driver, host):
        """
        Register a driver with the factory. Should only be called in this file.

        Parameters
        ----------
        driver: str
            A string identifying the driver.
        host: SensorHost
            The host driver to register.
        """
        self.__available_hosts[driver] = host

    def get(self, driver, id, hostname, port, *_args, **_kwargs):  # pylint: disable=redefined-builtin,invalid-name
        """
        Look up the driver for a given database entry. Raises a `ValueError` if
        the driver is not registered.

        Parameters
        ----------
        driver: str
            A string identifying the driver.
        id: uuid.UUID
            The uuid of the host configuration.
        hostname: str
            The ethernet hostname
        port: int
            The port of the host

        Returns
        -------
        SensorHost
            A sensor registered sensor host

        Raises
        ----------
        ValueError
        """
        host = self.__available_hosts.get(driver)
        if host is None:
            raise ValueError(f"No driver available for {driver}")
        return host(uuid=id, hostname=hostname, port=port)


host_factory = SensorHostFactory()
host_factory.register(driver="tinkerforge", host=TinkerforgeSensorHost)
