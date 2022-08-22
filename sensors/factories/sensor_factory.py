"""
This file contains the GPIB device factory, that produces sensor devices using
available special GPIB device drivers.
"""
from typing import Any

from errors import UnknownDriverError
from sensors.drivers.generic_scpi_driver import GenericScpiDriver
from sensors.drivers.hp3478a_driver import Hp3478ADriver


class SensorFactory:
    """
    A sensor factory to select the correct driver for given database
    config.
    """

    def __init__(self):
        self.__available_drivers = {}

    def register(self, cls: Any) -> None:
        """
        Register a driver with the factory. Should only be called in this file.

        Parameters
        ----------
        cls: Any
            The driver class to register.
        """
        self.__available_drivers[cls.driver] = cls

    def has(self, driver: str):
        """Returns true if the driver has been registered

        Parameters
        """
        return driver in self.__available_drivers

    def get(self, driver: str, connection: Any, **kwargs: Any) -> Any:
        """
        Look up the driver for a given database entry. Raises a `ValueError` if
        the driver is not registered.

        Parameters
        ----------
        driver: str
            A string identifying the driver.
        connection: Any
            The host connection

        Returns
        -------
        Any
            A sensor device driver object

        Raises
        ----------
        ValueError
            Raised if the device driver is not registered
        """
        try:
            device = self.__available_drivers[driver]
        except KeyError:
            raise UnknownDriverError(f"No driver available for {driver}") from None
        else:
            return device(connection=connection, **kwargs)


sensor_factory = SensorFactory()
sensor_factory.register(cls=GenericScpiDriver)
sensor_factory.register(cls=Hp3478ADriver)
