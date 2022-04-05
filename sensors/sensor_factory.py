# -*- coding: utf-8 -*-
"""
This file contains the GPIB device factory, that produces sensor devices using
available special GPIB device drivers.
"""
from hp3478a_async import HP_3478A
from .drivers.generic_gpib_device import GenericGpibDevice
from .drivers.generic_scpi_device import GenericDevice


class SensorFactory:
    """
    A sensor factory to select the correct driver for given database
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
        device: GenericDevice
            The device driver to register.
        """
        self.__available_hosts[driver] = device

    def get(self, driver, uuid, connection, event_bus):
        """
        Look up the driver for a given database entry. Raises a `ValueError` if
        the driver is not registered.

        Parameters
        ----------
        driver: str
            A string identifying the driver.
        uuid: Uuid
            The uuid of the sensor
        connection: Any
            The host connection
        event_bus: AsyncEventBus
            The global event bus used to communicate between devices

        Returns
        -------
        GenericDevice
            A sensor device, which inherits from GenericDevice

        Raises
        ----------
        ValueError
            Raised if the device driver is not registered
        """
        device = self.__available_hosts.get(driver)(uuid, connection, event_bus)
        if device is None:
            raise ValueError(f"No driver available for {driver}")
        return device


sensor_factory = SensorFactory()
sensor_factory.register(driver="generic_scpi", device=GenericDevice)
sensor_factory.register(driver="hp3478a", device=HP_3478A)
