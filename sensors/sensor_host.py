# -*- coding: utf-8 -*-
"""
This file contains the base class for all sensor hosts.
"""
from abc import ABC, abstractmethod


EVENT_BUS_BASE = "/hosts"
EVENT_BUS_ADD_HOST = EVENT_BUS_BASE + "/add_host"
EVENT_BUS_CONFIG_UPDATE = EVENT_BUS_BASE + "/by_uuid/{uuid}/update"
EVENT_BUS_DISCONNECT = EVENT_BUS_BASE + "/by_uuid/{uuid}/disconnect"
EVENT_BUS_ADD_SENSOR = EVENT_BUS_BASE + "/by_uuid/{uuid}/add_sensor"


class SensorHost(ABC):
    """
    The base class for all hosts connected via ethernet.
    """

    @property
    @abstractmethod
    def driver(self):
        """
        Returns
        -------
        str
            A string describing the driver used by the host.
        """

    @property
    def uuid(self):
        """
        Returns the globally unique id of the host used to identidy the host
        system-wide.

        Returns
        -------
        uuid.UUID
            The uuid of the database config.
        """
        return self.__uuid

    @property
    def hostname(self):
        """
        Returns
        -------
        str
            The hostname of the sensor host.
        """
        return self.__hostname

    @property
    def port(self):
        """
        Returns
        -------
        int
            The port of connection.
        """
        return self.__port

    def __init__(self, uuid, hostname, port):
        self.__uuid = uuid
        self.__hostname = hostname
        self.__port = port
