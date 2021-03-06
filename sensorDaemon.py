#!/usr/bin/env python
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

__version__ = "2.7.0"

import argparse
import psycopg2
import os
import signal
import sys
import time
import warnings

from configParser import ConfigParser
from daemon import Daemon
from daemonLogger import DaemonLogger
from sensors.sensorHost import SensorHost

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
    'insert_data' : "INSERT INTO sensor_data (time ,sensor_id ,value) VALUES (NOW(), (SELECT id FROM sensors WHERE sensor_uid=%s and enabled), %s)",
    'select_period': "SELECT callback_period FROM sensors WHERE sensor_uid=%s AND enabled",
    'select_hosts' : "SELECT hostname, port FROM sensor_nodes WHERE id IN (SELECT DISTINCT node_id FROM sensors WHERE enabled)"
}


class SensorDaemon(Daemon):
    """
    Main daemon, that runs in the background and monitors all sensors. It will configure them according to options set in the database and
    then place the returned data in the database as well.
    """
    def __get_hosts(self):
        """
        Reads hosts from the database and returns a list of host nodes.
        """
        hosts = {}
        postgrescon = None
        options = self.config.postgres
        self.logger.info("Fetching Brick Daemon hosts from database on '{host}:{port}'...".format(host=options['host'], port=options['port']))
        try:
            postgrescon = psycopg2.connect(host=options['host'], port=int(options['port']), user=options['username'], password=options['password'], dbname=options['database'])
            cur = postgrescon.cursor()
            cur.execute(POSTGRES_STMS['select_hosts'])
            rows = cur.fetchall()
            # Iterate over all hosts found in the database and create host object for them.
            for row in rows:
                self.logger.debug('Found host "%s:%s"', row[0], row[1])
                hosts[row[0]] = SensorHost(row[0], row[1], self)
        except psycopg2.DatabaseError as e:
            self.logger.critical('Error. Cannot get hosts from database: %s.', e)
            sys.exit(1)
        finally:
            if postgrescon:
                postgrescon.close()

        return hosts

    def get_callback_period(self, sensor_uid):
        """
        Called by each sensor through its host to query for the callback period. This is the minimum intervall a node is allowed to return data.
        """
        postgrescon = None
        options = self.config.postgres
        self.logger.info("Fetching Brick Daemon hosts from database on '{host}:{port}'...".format(host=options['host'], port=options['port']))
        try:
            postgrescon = psycopg2.connect(host=options['host'], port=int(options['port']), user=options['username'], password=options['password'], dbname=options['database'])
            cur = postgrescon.cursor()
            cur.execute(POSTGRES_STMS['select_period'], (sensor_uid, ))
            row = cur.fetchone()
            if row is not None:
                period = row[0]
                self.logger.debug('Period for sensor "%s" is %s ms', sensor_uid, period)
            else:
                self.logger.error('Error. Sensor "%s" not found in database. Please add the uid to the database to enable logging of this sensor.', sensor_uid)
                period = 0
        except psycopg2.DatabaseError as e:
            self.logger.critical('Error. Cannot get hosts from database: %s.', e)
            sys.exit(1)
        finally:
            if postgrescon:
                postgrescon.close()

        if isinstance(period, int):
            return period
        else:
            return 0

    @property
    def logger(self):
        """
        Returns the logger to send messages to the console/file
        """
        return self.__logger

    @property
    def config(self):
        """
        Returns the configParser object, which contains all options read from the config file
        """
        return self.__config

    def host_callback(self, sensor_type, sensor_uid, value, previous_update):
        """
        Callback function used by the sensors to write data to the database. It will be called by the host.
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
            self.logger.debug('Successfully written to database: value %s from sensor %s.', value, sensor_uid)
        except psycopg2.DatabaseError as e:
            if postgrescon:
                postgrescon.rollback()
            self.logger.error('Error. Cannot insert value "%s" from sensor "%s" into database: %s.', value, sensor_uid, e)
        finally:
            if postgrescon:
                postgrescon.close()

    def __init__(self, config):
        """
        Creates a sensorDaemon object.
        config: A configParser object containing all the configuration options needed
        """
        super(SensorDaemon, self).__init__('/tmp/daemon-example.pid')
        self.__config = config
        self.__logger = DaemonLogger(config.logging).get_logger()
        # Hook onto the SIGTERM (standard kill command) and SIGINT (Ctrl + C) signal
        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

    def run(self):
        """
        Start the daemon and keep it running through the while (True) loop. Execute shutdown() to kill it.
        """
        self.logger.warning("##################################################")
        self.logger.warning("Starting Daemon...")
        self.logger.warning("##################################################")

        # Retrieve all hosts from the database
        self.hosts = self.__get_hosts()
        # Stop the daemon if no hosts where found
        if (len(self.hosts) == 0):
            self.logger.error("No hosts where found in the database...")
            sys.exit(1)

        # Connect to all hosts. The connection will be kept open.
        for host in self.hosts.values():
            self.logger.warning('Connecting to brick daemon on "%s"...', host.host_name)
            self.logger.info("-----------------")
            host.connect()

        # Check for all host in regular intervals. If a host was previously unavailable, we will try to reconnect.
        while (True):
            time.sleep(self.config.sensors['ping_intervall'])
            for host in self.hosts.values():
                host.ping()

    def shutdown(self, sig_number, frame):
        """
        Stops the daemon and gracefully disconnect from all clients.
        """
        self.logger.warning("##################################################")
        self.logger.warning("Stopping Daemon...")
        self.logger.warning("##################################################")
        for host in self.hosts.values():
            host.disconnect()
        # Shutdown logging system
        sys.exit(0)

if __name__ == "__main__":
    """
    Load the config file, then start the daemon
    """
    parser = argparse.ArgumentParser(description='Sensor daemon for tinkerforge bricklets')
    parser.add_argument('action', nargs='?', choices=['start', 'stop', 'restart'], help='Run, stop or restart the service')
    parser.add_argument('--nodaemon', action='store_true', help='If this option is set no action is required. The deamon will not fork to the background.')
    args = parser.parse_args()

    config = ConfigParser(CONFIG_PATH)

    daemon = SensorDaemon(config)
    if (args.nodaemon):
        daemon.run()
    elif args.action == 'start':
        daemon.start()
    elif args.action == 'stop':
        daemon.stop()
    elif args.action == 'restart':
        daemon.restart()
    else:
        parser.print_usage()
