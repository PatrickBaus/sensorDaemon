# -*- coding: utf-8 -*-
"""
This file contains the base class for all sensor hosts.
"""


class SensorHost:
    """
    The base class for all hosts connected via ethernet.
    """

    @property
    def uuid(self):
        """
        Returns the globally unique id of the host used to identidy the host
        system-wide

        Returns
        -------
        uuid.UUID
            The uuid of the database config
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
