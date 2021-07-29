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
import asyncpg
import os
import json
import logging
import signal
import sys
import time
import warnings

from configParser import parse_config_from_env, parse_config_from_file
from daemon import Daemon
from sensors.sensorHost import host_factory

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
mock_database = {
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


class MockDatabase():
    def __init__(self):
        self.__logger = logging.getLogger(__name__)

    async def get_hosts(self):
        for host in mock_database['hosts']:
            host['config'] = json.loads(host['config'])
            yield host
            await asyncio.sleep(0)

    async def get_sensor_config(self, uid):
        for config in mock_database['sensor_config'].get(uid, (None, )):
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


class DatabaseManager():
    def __init__(self):
        self.__logger = logging.getLogger(__name__)
        self.__database = MockDatabase()

    async def run(self):
        pass

    def get_sensor_config(self, pid):
        return self.__database.get_sensor_config(pid)

    @property
    def hosts(self):
        return self.__database.get_hosts()

    async def disconnect(self):
        pass


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


class SensorDaemon():
    """
    Main daemon, that runs in the background and monitors all sensors. It will
    configure them according to options set in the database and then place the
    returned data in the database as well.
    """
    @property
    def config(self):
        """
        Returns the configParser object, which contains all options read from
        the config file
        """
        return self.__config

    def __init__(self, config):
        """
        Creates a sensorDaemon object.
        config: A config dict containing the configuration options
        """
        self.__config = config
        self.__logger = logging.getLogger(__name__)
        self.__shutting_down = asyncio.Event()
        self.__shutting_down.clear()

    async def __get_hosts(self):
        """
        Reads hosts from the database and returns a list of host nodes.
        """
        return
        postgrescon = None
        options = self.config["postgres"]
        self.__logger.info("Fetching sensor hosts from database on '{host}:{port}'...".format(host=options['host'], port=options['port']))

        try:
            postgrescon = await asyncpg.connect(host=options['host'], port=int(options['port']), user=options['username'], password=options['password'], database=options['database'])
            stmt = await postgrescon.prepare(POSTGRES_STMS['select_hosts'])
            async with postgrescon.transaction():
                async for record in stmt.cursor():
                    self.__logger.debug('Found host "%s:%s"', record["hostname"], record["port"])
                    hosts[record["hostname"]] = host_factory.get(driver=record["driver"], hostname=record["hostname"], port=record["port"], parent=self)
        except asyncpg.InterfaceError as e:
            self.__logger.critical('Error. Cannot get hosts from database: %s.', e)
            raise
        finally:
            if postgrescon is not None:
                await postgrescon.close()

    async def get_sensor_config(self, sensor_uid):
        """
        Called by each sensor through its host to query for its config.
        """
        return self.__database.get_sensor_config(sensor_uid)

    def host_callback(self, sensor_type, sensor_uid, value, previous_update):
        """
        Callback function used by the sensors to write data to the database.
        It will be called by the host.
        sensor_type: String that states the type of sensor
        sensor_uid: String that represents the sensors unique id (embedded in the flash rom)
        value: Float to be written to the database
        previous_update: Integer representing the number of ms between this callback and the previous.
            If there was no previous update, set this to None.
        """
        postgrescon = None
        options = self.config.postgres
        try:
            postgrescon = psycopg2.connect(host=options['host'], port=int(options['port']), user=options['username'], password=options['password'], dbname=options['database'])
            cur = postgrescon.cursor()
            cur.execute(POSTGRES_STMS['insert_data'], (sensor_uid, value))
            # Only works on InnoDB databases, not needed on MyISAM tables, but it doesn't hurt. On MyISAM tables data will be committed immediately.
            postgrescon.commit()
            self.__logger.debug('Successfully written to database: value %s from sensor %s.', value, sensor_uid)
        except psycopg2.DatabaseError as e:
            if postgrescon:
                postgrescon.rollback()
            self.__logger.error('Error. Cannot insert value "%s" from sensor "%s" into database: %s.', value, sensor_uid, e)
        finally:
            if postgrescon:
                postgrescon.close()

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
        await asyncio.gather(
            self.__host_manager.disconnect(),
            self.__database_manager.disconnect()
        )
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
