# -*- coding: utf-8 -*-
# ##### BEGIN GPL LICENSE BLOCK #####
#
# Copyright (C) 2020  Patrick Baus
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# ##### END GPL LICENSE BLOCK #####

from abc import ABCMeta, abstractmethod


class SensorHost(metaclass=ABCMeta):

    @property
    def hostname(self):
        """
        Returns the hostname of the Tinkerforge brick daemon, where this host can be found
        """
        return self.__hostname

    @property
    def port(self):
        """
        Returns the port at which the Tinkerforge brick daemon is listening
        """
        return self.__port

    @property
    def config(self):
        """
        Returns the configuration of the host
        """
        return self.__config

    @property
    def parent(self):
        """
        Returns the sensor daemon object.
        """
        return self.__parent

    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def disconnect(self):
        pass

    async def process_value(self, pid, sid, value):
        print("foo", pid, sid, value)

    def __init__(self, hostname, port, config, parent):
        self.__hostname = hostname
        self.__port = port
        self.__config = config
        self.__parent = parent
