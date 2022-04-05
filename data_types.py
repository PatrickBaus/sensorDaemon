# -*- coding: utf-8 -*-
"""
This file contains all custom data types used accross the application
"""
from datetime import timezone, datetime
from enum import Enum, auto


class ChangeEvent:  # pylint: disable=too-few-public-methods
    """
    The base class to encapsulate any configuration/system change. The type of
    change is determined by inheritance.
    """
    @property
    def change(self):
        """
        Returns
        -------
        Any
            The changed data.
        """
        return self.__change

    def __init__(self, change):
        self.__change = change


class UpdateChangeEvent(ChangeEvent):   # pylint: disable=too-few-public-methods
    """
    An update to a configuration.
    """


class AddChangeEvent(ChangeEvent):  # pylint: disable=too-few-public-methods
    """
    A new item to add to the configuration.
    """

class RemoveChangeEvent(ChangeEvent):   # pylint: disable=too-few-public-methods
    """
    Configuration to be removed.
    """
    def __init__(self):
        super().__init__(None)


class DataEvent:  # pylint: disable=too-few-public-methods
    """
    The base class to encapsulate any data event.
    """

    @property
    def timestamp(self):
        """
        Returns
        -------
        float
            A POSIX timestamp counting seconds since the epoch in UTC.
        """
        return self.__timestamp

    @property
    def sender(self):
        """
        Returns
        -------
        uuid
            The sender of the data.
        """
        return self.__sender

    @property
    def sid(self):
        """
        Returns
        -------
        int
            The secondary id of the sender.
        """
        return self.__sid


    @property
    def topic(self):
        """
        Returns
        -------
        str
            The PubSub topic, where the data is to be published.
        """
        return self.__topic

    @property
    def value(self):
        """
        Returns
        -------
        Any
            The data.
        """
        return self.__value

    @property
    def driver(self):
        """
        Returns
        -------
        str
            The name of the driver used to produce the data
        """
        return self.__driver

    def __init__(self, sender, sid, driver, topic, value):
        self.__timestamp = datetime.now(timezone.utc).timestamp()
        self.__sender = sender
        self.__sid = sid
        self.__topic = topic
        self.__value = value
        self.__driver = driver

    def __str__(self):
        return f"Data event from {self.__sender}: {self.__value}"


class ChangeType(Enum):
    """
    The type of changes sent out by the database.
    """
    ADD = auto()
    REMOVE = auto()
    UPDATE = auto()
