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

from .tinkerforge_host import TinkerforgeSensorHost


class SensorHostFactory:
    def __init__(self):
        self.__available_hosts = {}

    def register(self, driver, host):
        self.__available_hosts[driver] = host

    def get(self, driver, hostname, port, config, parent):
        host = self.__available_hosts.get(driver)
        if host is None:
            raise ValueError(f"No driver available for {driver}")
        return host(hostname, port, config, parent)


host_factory = SensorHostFactory()
host_factory.register(driver="tinkerforge", host=TinkerforgeSensorHost)
