#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ##### BEGIN GPL LICENSE BLOCK #####
#
# Copyright (C) 2021  Patrick Baus
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
import os
import logging
import signal
import sys
import warnings

from configParser import parse_config_from_env, parse_config_from_file
from daemon import Daemon
from sensors.host_factory import host_factory
from managers import DatabaseManager, HostManager

from _version import __version__


def we_are_frozen():
    """Returns whether we are frozen via py2exe.
    This will affect how we find out where we are located."""

    return hasattr(sys, "frozen")


def module_path():
    """ This will get us the program's directory,
    even if we are frozen using py2exe"""

    if we_are_frozen():
        return os.path.dirname(sys.executable)

    return os.path.dirname(__file__)


CONFIG_PATH = module_path() + '/sensors.conf'

POSTGRES_STMS = {
    'insert_data': "INSERT INTO sensor_data (time ,sensor_id ,value) VALUES (NOW(), (SELECT id FROM sensors WHERE sensor_uid=%s and enabled), %s)",
    'select_sensor_config': "SELECT config FROM sensors WHERE sensor_uid=%s AND enabled",
    'select_hosts': "SELECT hostname, port, (SELECT drivers.name FROM drivers WHERE drivers.id=driver_id) as driver FROM sensor_nodes WHERE id IN (SELECT DISTINCT node_id FROM sensors WHERE enabled)"
}


class SensorDaemon():
    """
    Main daemon, that runs in the background and monitors all sensors. It will
    configure them according to options set in the database and then place the
    returned data in the database as well.
    """
    def __init__(self, config):
        """
        Creates a sensorDaemon object.
        config: A config dict containing the configuration options
        """
        self.__config = config
        self.__logger = logging.getLogger(__name__)
        self.__shutting_down = asyncio.Event()
        self.__shutting_down.clear()

    async def run(self):
        """
        Start the daemon and keep it running through the while (True)
        loop. Execute shutdown() to kill it.
        """
        self.__logger.warning("#################################################")
        self.__logger.warning("Starting Daemon...")
        self.__logger.warning("#################################################")

        # Catch signals and shutdown
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for sig in signals:
            asyncio.get_running_loop().add_signal_handler(
                sig, lambda: asyncio.create_task(self.shutdown()))

        # Start database manager
        self.__database_manager = DatabaseManager()
        asyncio.create_task(self.__database_manager.run())

        # Start host manager
        self.__host_manager = HostManager(self.__database_manager)
        asyncio.create_task(self.__host_manager.connect())

        await self.__shutting_down.wait()

    async def shutdown(self):
        """
        Stops the daemon and gracefully disconnect from all clients.
        """
        self.__logger.warning("#################################################")
        self.__logger.warning("Stopping Daemon...")
        self.__logger.warning("#################################################")
        try:
            await asyncio.gather(
                self.__host_manager.disconnect(),
                self.__database_manager.disconnect()
            )
        except BaseException:
            self.__logger.exception("Error during shutdown")

        self.__shutting_down.set()  # Kills the run() call

        # Get all running tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        # and stop them
        [task.cancel() for task in tasks]
        # finally wait for them to terminate
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        except Exception:
            self.__logger.exception("Error while reaping tasks during shutdown")


async def main():
    config = load_config()
    daemon = SensorDaemon(config)
    try:
        await daemon.run()
    except asyncio.CancelledError:
        pass


def load_config():
    """
    Tries to load the config either from a file or from environment variables
    """
    if os.path.isfile(CONFIG_PATH):
        return parse_config_from_file(CONFIG_PATH)
    else:
        return parse_config_from_env()


# Report all mistakes managing asynchronous resources.
warnings.simplefilter('always', ResourceWarning)
logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    level=logging.INFO,    # Enable logs from the ip connection. Set to debug for even more info
    datefmt='%Y-%m-%d %H:%M:%S'
)

asyncio.run(main(), debug=True)
