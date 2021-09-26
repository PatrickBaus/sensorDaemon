# -*- coding: utf-8 -*-

from hp3478a_async import HP_3478A


class GpibDeviceFactory:
    """
    A senor host factory to select the correct driver for given database
    config.
    """
    def __init__(self):
        self.__available_hosts = {}

    def register(self, driver, device):
        """
        Register a driver with the factory. Should only be called in this file.

        Parameters
        ----------
        driver: str
            A string identifying the driver.
        host: SensorHost
            The host driver to register.
        """
        self.__available_hosts[driver] = device

    def get(self, driver, connection):
        """
        Look up the driver for a given database entry. Raises a `ValueError` if
        the driver is not registered.

        Parameters
        ----------
        driver: str
            A string identifying the driver.

        connection: Any
            The IP connection

        Returns
        -------
        SensorHost
            A sensor registered sensor host

        Raises
        ----------
        ValueError
        """
        device = self.__available_hosts.get(driver)(connection)
        if device is None:
            raise ValueError(f"No driver available for {driver}")
        return device


gpib_device_factory = GpibDeviceFactory()
gpib_device_factory.register(driver="hp3478a", device=HP_3478A)
