#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ##### BEGIN GPL LICENSE BLOCK #####
#  
# Copyright (C) 2016  Patrick Baus
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
#
# Changelog:
#
# 28.11.2016 2.4.2
# - Added port option to MYSQL connection
#
# 21.07.2016 2.4.1
# - Added port number of connected sensor to log output
# - Only monitor sensors, which are enabled
#
# 09.04.2015 2.4.0
# - Added the Linux daemon double fork trick to daemonize this program.
#
# 06.01.2015 2.3.8
# - Added PTC bricklet
# - Added error message in case of unknown sensors
# - Updated copyright notice
# - Changed console colour of CRITICAL message to red (from yellow)
# - Changed warning level of sensor initialisation errors from critical to error
#
# 03.09.2014 2.3.7
# - Fixed some typos in sensorHost.py
#
# 03.09.2014 2.3.6
# - Added colour to the console log
# - Added more debug logging, when writing to the database
# - Changed all objects to the new python style objects
# - Changed some logic concerning early callbacks. We will now throw away ALL sensor values, that come in way too early, that means before their set callback duration (dt < dt_set * 0.9)!
# - Changed some getters to properties
# - Removed python environment from configParser.py
# - Fixed barometer bricklet
# - Fixed humidity bricklet
# - Fixed typo
# - Fixed a bug that caused reconnection attemps to silently fail
#
# 10.04.2014 2.3.5
# - Fixed incorrectly named function in sensorHost.py
#
# 19.12.2013 2.3.4
# - Fixed the barometer sensor plugin
#
# 03.11.2013 2.3.3
# - Fixed a bug which caused sensors to hang indefinitely, when it timed out due to network latency.
#
# 21.06.2013 2.3.2
# - reverted change in 2.0.7. UTC_TIMESTAMP() is no longer used, because of bug http://bugs.mysql.com/bug.php?id=58583
#
# 27.09.2013 2.3.1
# - Fixed typos
#
# 22.09.2013 2.3.0
# - Changed formatting to conform to PEP-8
# - Renamed all private class variables (This will break compatibility!)
# - Added installer for Windows
#
# 18.09.2013 2.2.0
# - Script will now terminate if no hosts where found
# - Now fully Plug 'n' Play compatible 
# - Unconfigured sensors will now be gracefully handled
# - Added some more error handling
#
# 27.08.2013 2.1.1
# - Changed temperature bricklet unit from cK to K.
#
# 08.07.2013 2.1.0
# - Licensed under GPL v3
# - Moved configuration to external conf file
# - Updated sensors to new API naming scheme
# - Added ambient light sensor
# - Added some more output
# - Improved uncaught exception handling
#
# 21.06.2013 2.0.7
# - changed database timestamp from local time to UTC
#
# 13.06.2013 2.0.6
# - Changed log level for unknown hosts to error
#
# 12.06.2013 2.0.5
# - Fixed some typos
# - Fixed crash when sensor host could not be found
# - Added sensor type to enumeration message
# - Removed master brick from enumeration messages
#
# 28.02.2013 2.0.4
# - Fixed incorrect display of the last update variable, which was given in ms but displayed as seconds
#
# 25.02.2013 2.0.3
# - Added more output on mysql errors
# - Fixed a bug, that could cause clients to throw a timeout exception
#
# 24.02.2013 2.0.2
# - Fixed enumeartion on reconnect
# - Added "" to the output of sensor names
#
# 21.02.2013 2.0.1
# - Renamed table "sensor_location" to "sensor_nodes"
#
# 08.02.2013 2.0
# - Moved to callbacks for all supported bricks and bricklets
#
# 24.01.2013 1.5
# - Added support for multiple brickd hosts
# - Dynamically load the hosts from a MySQL database
#
# 24.01.2013 1.4.1
# - Added more debugging output
#
# 23.01.2013 1.4
# - Moved to Tinkerforge API 2.0
#
# 05.12.2012 Version 1.3.7
# - Fixed an exception in case the sensors do not return a result
#
# 03.12.2012 Version 1.3.6
# - Fixed bug in case the MySQL database does not respond
#
# 30.10.2012 Version 1.3.5
# - Optimized MySQL inserts in main
#
# 28.10.2012 Version 1.3.4
# - Added more logging to the MySQL insert statement in main
#
# 22.10.2012 Version 1.3.3
# - Added sensors count to the info output
# - Fixed a bug when initialising a new sensor, the logger will now be corretly set
#
# 08.10.2012 Version 1.3.2
# - Convert MySQL warnings to errors to raise and catch them via try/except
#
# 01.10.2012 Version 1.3.1
# - Moved the number to retries to the global variable RETRY_COUNT, even for sensors
# - Added logger to sensors
#
# 01.10.2012 Version 1.3
# - Added logging to file and console
#
# 01.10.2012 Version 1.2:
# - Added multithreading to ipcon assignment
#
# 01.10.2012 Version 1.1:
# - Added error handler
# - Added cleanup of MySQL connection after use
# - Improved error handling
# - Added more output
#
# 26.10.2012 Version 1.0:
# - Initial Release
#

__version__ = "2.3.7" + " rev. " + filter(str.isdigit, "$Revision: 327 $")

import MySQLdb
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
        return os.path.dirname(unicode(sys.executable, sys.getfilesystemencoding( )))

    return os.path.dirname(unicode(__file__, sys.getfilesystemencoding( )))

CONFIG_PATH = module_path() + '/sensors.conf'

MYSQL_STMS = {
    'insert_data' : "INSERT INTO `sensor_data` (`date` ,`sensor_id` ,`value`) VALUES (NOW(), (SELECT `id` FROM `sensors` WHERE sensor_uid=%s and enabled), %s)",
    'select_period': "SELECT callback_period FROM `sensors` WHERE sensor_uid=%s AND enabled",
    'select_hosts' : "SELECT hostname, port FROM `sensor_nodes`"
}

class SensorDaemon(Daemon):
    """
    Main daemon, that runs in the background and monitors all sensors. It will configure them according to options set in the MySQL database and
    then place the returned data in the database as well.
    """
    def __get_hosts(self):
        """
        Reads hosts from the database and returns a list of host nodes.
        """
        hosts = {}
        mysqlcon = None
        # Retrieve all data necessary for the MySQL connection
        mysql_options = self.config.mysql
        self.logger.info("Fetching Brick Daemon hosts from database on '{host}:{port}'...".format(host=mysql_options['host'], port=mysql_options['port']))
        try:
            mysqlcon = MySQLdb.connect(host=mysql_options['host'], port=int(mysql_options['port']), user=mysql_options['username'], passwd=mysql_options['password'], db=mysql_options['database'])
            cur = mysqlcon.cursor()
            cur.execute(MYSQL_STMS['select_hosts'])
            rows = cur.fetchall()
            # Iterate over all hosts found in the database and create host object for them.
            for row in rows:
                self.logger.debug('Found host "%s:%s"', row[0], row[1])
                hosts[row[0]] = SensorHost(row[0], row[1], self)
        except MySQLdb.Error, e:
            self.logger.critical('Error. Cannot get hosts from MySQL database: %s.', e)
            sys.exit(1)
        finally:
            if mysqlcon:
                mysqlcon.close()
            self.logger.info("Found %d host%s.", len(hosts), "s"[len(hosts)==1:])
        return hosts

    def get_callback_period(self, sensor_uid):
        """
        Called by each sensor through its host to query for the callback period. This is the minimum intervall a node is allowed to return data.
        """
        mysqlcon = None
        period = 0
        self.logger.debug('Fetching callback period for sensor "%s"...', (sensor_uid))
        try:
            # Retrieve all data necessary for the MySQL connection
            mysql_options = self.config.mysql
            mysqlcon = MySQLdb.connect(host=mysql_options['host'], port=int(mysql_options['port']), user=mysql_options['username'], passwd=mysql_options['password'], db=mysql_options['database'])
            cur = mysqlcon.cursor()
            cur.execute(MYSQL_STMS['select_period'], sensor_uid)
            row = cur.fetchone()
            if row is not None:
                period = row[0]
                self.logger.debug('Period for sensor "%s" is %s ms', sensor_uid, period)
            else:
                self.logger.error('Error. Sensor "%s" not found in MySQL database. Please add the uid to the database to enable logging of this sensor.', sensor_uid)
                period = 0
        except MySQLdb.Error, e:
            self.logger.error('Error. Cannot get callback period for sensor "%s" from MySQL database: %s', sensor_uid, e)
        finally:
            if mysqlcon:
                mysqlcon.close()

        if isinstance(period, (int, long)):
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

    def host_callback(self, sensor_type, sensor_uid, value, last_update):
        """
        Callback function used by the sensors to write data to the database. It will be called by the host.
        sensor_type: String that states the type of sensor
        sensor_uid_: String that represents the sensors unique id (embedded in the flash rom)
        value: Float to be written to the database
        last_update: Integer representing the number of ms between this callback and the previous.
        If there was  no last update, set this to None.
        """
        mysqlcon = None
        if (last_update is None):
            self.logger.info('%s sensor "%s" returned value %s.', sensor_type.title(), sensor_uid, value)
        else:
            self.logger.info('%s sensor "%s" returned value %s. Last update was %i s ago.', sensor_type.title(), sensor_uid, value, last_update / 1000)
        try:
            mysql_options = self.config.mysql
            mysqlcon = MySQLdb.connect(host=mysql_options['host'], port=int(mysql_options['port']), user=mysql_options['username'], passwd=mysql_options['password'], db=mysql_options['database'])
            cur = mysqlcon.cursor()
            cur.execute(MYSQL_STMS['insert_data'], (sensor_uid, value))
            # Only works on InnoDB databases, not needed on MyISAM tables, but it doesn't hurt. On MyISAM tables data will be committed immediately.
            mysqlcon.commit()
            self.logger.debug('Successfully written to database: value %s from sensor %s.', value, sensor_uid)
        except MySQLdb.Error, e:
            if mysqlcon:
                # In case of an error roll back any changes. This is only possible on InnoDB. MyISAM will committ any change immediately.
                mysqlcon.rollback()
            self.logger.error('Error. Cannot insert value "%s" from sensor "%s" into MySQL database: %s.', value, sensor_uid, e)
        finally:
            if mysqlcon:
                mysqlcon.close()

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
        for host in self.hosts.itervalues():
            self.logger.warning('Connecting to brick daemon on "%s"...', host.host_name)
            self.logger.info("-----------------")
            host.connect()

        # Check for all host in regular intervals. If a host was previously unavailable, we will try to reconnect.
        while (True):
            time.sleep(self.config.sensors['ping_intervall'])
            for host in self.hosts.itervalues():
                host.ping()

    def shutdown(self, sig_number, frame):
        """
        Stops the daemon and gracefully disconnect from all clients.
        """
        self.logger.warning("##################################################")
        self.logger.warning("Stopping Daemon...")
        self.logger.warning("##################################################")
        for host in self.hosts.itervalues():
            host.disconnect()
        # Shutdown logging system    
        sys.exit(0)

if __name__ == "__main__":
    """
    Load the config file, then start the daemon
    """
    config = ConfigParser(CONFIG_PATH)

    daemon = SensorDaemon(config)
#    daemon.run()
    if len(sys.argv) == 2:
       if 'start' == sys.argv[1]:
          daemon.start()
       elif 'stop' == sys.argv[1]:
          daemon.stop()
       elif 'restart' == sys.argv[1]:
          daemon.restart()
       else:
          print "Unknown command"
          sys.exit(2)
       sys.exit(0)
    else:
       print "usage: %s start|stop|restart" % sys.argv[0]
       sys.exit(2)
