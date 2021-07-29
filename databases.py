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

import asyncio
import logging
import json


class MockDatabase():
    __mock_database = {
        'hosts': [
            {
                'hostname': '10.0.0.5',
                'port': 4223,
                'driver': 'tinkerforge',
                'config': '{"on_connect": []}',
            },
            {
                'hostname': '192.168.1.152',
                'port': 4223,
                'driver': 'tinkerforge',
                'config': '{"on_connect": []}',
            },
            {
                'hostname': '127.0.0.1',
                'port': 4223,
                'driver': 'tinkerforge',
                'config': '{"on_connect": []}',
            },
        ],
        'sensor_config': {
            125633: [
                {
                    "sid": 0,
                    "period": 0,
                    "config": '{"on_connect": []}',
                },
                {
                    "sid": 1,
                    "period": 2000,
                    "config": '',
                }
            ],
            169087: [
                {
                    "sid": 0,
                    "period": 1000,
                    "config": '{"on_connect": [{"function": "set_status_led_config", "kwargs": {"config": 2} }, {"function": "set_status_led_config", "args": [2] } ]}',
                },
            ]
        }
    }

    def __init__(self):
        self.__logger = logging.getLogger(__name__)

    async def get_hosts(self):
        for host in self.__mock_database['hosts']:
            host['config'] = json.loads(host['config'])
            yield host
            await asyncio.sleep(0)

    async def get_sensor_config(self, uid):
        for config in self.__mock_database['sensor_config'].get(uid, (None, )):
            self.__logger.debug("Got config: %s", config)
            if config is None:
                yield None
            else:
                config_copy = config.copy()
                try:
                    if config_copy.get("config"):
                        config_copy["config"] = json.loads(config_copy["config"])
                    else:
                        config_copy["config"] = {}
                except json.decoder.JSONDecodeError as e:
                    self.__logger.error('Error. Invalid config for sensor: %i. Error: %s', uid, e)
                    config_copy["config"] = {}
                yield config_copy
            await asyncio.sleep(0)
