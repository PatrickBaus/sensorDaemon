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


class HostManager():
    def __init__(self, database):
        self.__logger = logging.getLogger(__name__)
        self.__database = database
        self.__hosts = {}

    def add_host(self, host):
        new_host = host_factory.get(
            driver=host['driver'],
            hostname=host['hostname'],
            port=host['port'],
            config=host['config'],
            parent=self,
        )
        # TODO: Only add a host, if is not already managed.
        self.__hosts[(host['hostname'], host['port'])] = new_host
        asyncio.create_task(new_host.run())

    def get_sensor_config(self, pid):
        self.__logger.debug("Getting sensor config for sensor id %i", pid)
        return self.__database.get_sensor_config(pid)

    async def connect(self):
        # Retrieve all hosts from the database
        # and connect
        async for host in self.__database.hosts:
            self.add_host(host)

    async def update_config(self, config_update):
        update_type, hosts = config_update
        if update_type == "remove":
            for host in hosts:
                if host in self.__hosts:
                    try:
                        await host.disconnect()
                    except Exception:
                        self.__logger.exception("Error removing host %s:%i", *host)
                        raise
                    finally:
                        del hosts[host]
        elif update_type == "add":
            pass
        elif update_type == "update":
            pass

    async def disconnect(self):
        tasks = [host.disconnect() for host in self.__hosts.values()]
        await asyncio.gather(*tasks)
