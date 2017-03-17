# -*- coding: utf-8 -*-
# ##### BEGIN GPL LICENSE BLOCK #####
#
# Copyright (C) 2015  Patrick Baus
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

from __future__ import absolute_import, division, print_function

import logging
import os
import sys
import yaml

def we_are_frozen():
    """Returns whether we are frozen via py2exe.
    This will affect how we find out where we are located."""

    return hasattr(sys, "frozen")


def module_path():
    """ This will get us the program's directory,
    even if we are frozen using py2exe"""

    if we_are_frozen():
        return os.path.dirname(sys.executable)

    return os.path.dirname(__file__, sys.getfilesystemencoding)

class ConfigParser(object):
    def uncaught_exception_handler(self,type, value, tb):
        self.getLogger().exception('Uncaught exception: %s', value)

    def __load_logging(self, config):
        """
        Load config parameters in the logging subsection and do some sanity checking
        """
        def string_to_level(value):
            """
            Maps a string to the corresponding enum.
            """
            if value == "info":
                log_llevel = logging.INFO
            elif value == "warning":
                log_level = logging.WARNING
            elif value == "error":
                log_level = logging.ERROR
            elif value == "critical":
                log_level = logging.CRITICAL
            elif value == "debug":
                log_level = logging.DEBUG
            else:
                log_level = None
            return log_level
            
        result = {}
        result['dateformat'] = config['dateformat']
        result['console_loglevel'] = string_to_level(config['console_loglevel'])
        result['file_loglevel'] = string_to_level(config['file_loglevel'])
        # If no absolute path is given, append the path name to the file directory
        if os.path.isabs(config['logfile']):
            result['logfile'] = config['logfile']
        else:
            result['logfile'] = module_path() + "/"  + config['logfile']

        return result

    def __load_default(self):
        """
        Load a default config
        """
        self.__options = __DEFAULT
        

    def __load_config(self, config_path):
        """
        Load config file. "configPath" is the full path name to the config file
        """
        self.__options = {'logging' : {}, 'mysql' : {}, 'sensors' : {}}
        config_file = open(config_path)
        config_map = yaml.safe_load(config_file)
        config_file.close()

        # Check the config for errors and abort in case of unresolved errors
        self.__check_config(config_map)

        # Setup logging
        self.__options['logging'] = self.__load_logging(config_map['logging'])

        # Setup MySQL
        self.__options['mysql'] = config_map['mysql']

        # Setup sensors
        self.__options['sensors'] = config_map['sensors']

    def __check_config(self, config_map):
        """
        Check config file for missing parameters
        """
        # Logging
        try:
            config_map['logging']['dateformat']
        except:
            sys.exit('Error: Logging option "dateformat" has not been set.')
        try:
            config_map['logging']['file_loglevel']
        except:
            sys.exit('Error: Logging option "file_loglevel" has not been set.')
        try:
            config_map['logging']['logfile']
        except:
            sys.exit('Error: Logging option "logfile" has not been set.')

        # MySQL
        try:
            config_map['mysql']['host']
        except:
            sys.exit('Error: MySQL option "host" has not been set.')
        try:
            config_map['mysql']['port']
        except:
            config_map['mysql']['port'] = 3306
        try:
            config_map['mysql']['username']
        except:
            sys.exit('Error: MySQL option "username" has not been set.')
        try:
            config_map['mysql']['password']
        except:
            sys.exit('Error: MySQL option "password" has not been set.')
        try:
            config_map['mysql']['database']
        except:
            sys.exit('Error: MySQL option "database" has not been set.')

        # Sensors
        try:
            config_map['sensors']['ping_intervall']
        except:
            sys.exit('Error: Sensor option "ping_intervall" has not been set.')

    @property
    def logging(self):
        """
        Returns the part of the configuration containing the logger config
        """
        return self.__options['logging']

    @property
    def mysql(self):
        """
        Returns the part of the configuration containing the MySQL config
        """
        return self.__options['mysql']

    @property
    def sensors(self):
        """
        Returns the part of the configuration containing the sensor config
        """
        return self.__options['sensors']

    def __init__(self, config_file):
        """
        Creates a configParser object.
        configFile: The absolute path to the configuration file to be loaded
        """
        try:
            self.__load_config(config_file)
        except FileNotFoundError:
            sys.exit('Error: Config file does not exist. "Check sensors.conf.default" for an example.')
