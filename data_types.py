# -*- coding: utf-8 -*-
"""
This file contains all custom data types used accross the application
"""

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


class ChangeType(Enum):
    """
    The type of changes sent out by the database.
    """
    ADD = auto()
    REMOVE = auto()
    UPDATE = auto()
