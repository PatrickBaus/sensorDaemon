# -*- coding: utf-8 -*-
# ##### BEGIN GPL LICENSE BLOCK #####
#
# Copyright (C) 2020  Patrick Baus
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

    return os.path.dirname(__file__)


string_to_level = {
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
    'debug': logging.DEBUG,
}


def parse_config_from_env():
    log = {}
    log["console_loglevel"] = string_to_level.get(os.getenv("CONSOLE_LOGLEVEL"), logging.NOTSET)
    log["file_loglevel"] = string_to_level.get(os.getenv("FILE_LOGLEVEL"), logging.NOTSET)
    log["dateformat"] = os.getenv("DATEFORMAT", "%b %d %H:%M:%S")
    log["logfile"] = os.getenv("LOGFILE", "sensor_daemon.log")
    if not os.path.isabs(log["logfile"]):
        log['logfile'] = module_path() + "/" + log['logfile']

    postgres = {}
    postgres["host"] = os.getenv("POSTGRES_HOST", "localhost")
    postgres["port"] = int(os.getenv("POSTGRES_PORT", "3306"))
    postgres["username"] = os.getenv("POSTGRES_USER", "sensors")
    postgres["password"] = os.getenv("POSTGRES_PASSWORD")
    postgres["database"] = os.getenv("POSTGRES_DATABASE", "sensors")
    if postgres["password"] is None:
        raise RuntimeError("Postgtres password not set")

    sensors = {}
    sensors["keepalive_interval"] = int(os.getenv("SENSORS_KEEPALIVE_INTERVAL", "60"))

    return {
      "logging": log,
      "postgres": postgres,
      "sensors": sensors,
    }


def parse_config_from_file(config):
    with open(config) as file:
        config_map = yaml.safe_load(file)

    log = {}
    log["console_loglevel"] = string_to_level.get(config_map["logging"]["console_loglevel"], logging.NOTSET)
    log["file_loglevel"] = string_to_level.get(config_map["logging"]["file_loglevel"], logging.NOTSET)
    log["dateformat"] = config_map["logging"].get("dateformat", "%b %d %H:%M:%S")
    log["logfile"] = config_map["logging"].get("logfile", "sensor_daemon.log")
    if not os.path.isabs(log["logfile"]):
        log['logfile'] = module_path() + "/" + log['logfile']

    postgres = {}
    postgres["host"] = config_map["postgres"].get("host", "localhost")
    postgres["port"] = int(config_map["postgres"].get("port", "3306"))
    postgres["username"] = config_map["postgres"].get("user", "sensors")
    postgres["password"] = config_map["postgres"].get("password")
    postgres["database"] = config_map["postgres"].get("database", "sensors")
    if postgres["password"] is None:
        raise RuntimeError("Postgtres password not set")

    sensors = {}
    sensors["keepalive_interval"] = int(config_map["sensors"].get("keepalive_intervall", "60"))

    return {
      "logging": log,
      "postgres": postgres,
      "sensors": sensors,
    }


class ConfigParser(object):
    def uncaught_exception_handler(self, type, value, tb):
        self.getLogger().exception('Uncaught exception: %s', value)

    def __load_logging(self, config):
        """
        Load config parameters in the logging subsection and do some sanity
        checking
        """
        string_to_level = {
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL,
          'debug': logging.DEBUG,
        }

        result = {}
        result['dateformat'] = config['dateformat']
        result['console_loglevel'] = string_to_level.get(config['console_loglevel'], logging.NOTSET)
        result['file_loglevel'] = string_to_level.get(config['file_loglevel'], logging.NOTSET)
        # If no absolute path is given, append the path name to the file directory
        if os.path.isabs(config['logfile']):
            result['logfile'] = config['logfile']
        else:
            result['logfile'] = module_path() + "/" + config['logfile']

        return result

    def __load_config(self, config_path):
        """
        Load config file. "configPath" is the full path name to the config file
        """
        self.__options = {'logging': {}, 'mysql': {}, 'postgres': {}, 'sensors': {}}
        config_file = open(config_path)
        config_map = yaml.safe_load(config_file)
        config_file.close()

        # Check the config for errors and abort in case of unresolved errors
        self.__check_config(config_map)

        # Setup logging
        self.__options['logging'] = self.__load_logging(config_map['logging'])

        # Setup MySQL
        self.__options['mysql'] = config_map['mysql']

        # Setup Postgres
        self.__options['postgres'] = config_map['postgres']

        # Setup sensors
        self.__options['sensors'] = config_map['sensors']

    def __check_config(self, config_map):
        """
        Check config file for missing parameters
        """
        # Logging
        try:
            config_map['logging']['dateformat']
        except Exception:
            sys.exit('Error: Logging option "dateformat" has not been set.')
        try:
            config_map['logging']['console_loglevel']
        except Exception:
            sys.exit('Error: Logging option "console_loglevel" has not been set.')
        try:
            config_map['logging']['file_loglevel']
        except Exception:
            sys.exit('Error: Logging option "file_loglevel" has not been set.')
        try:
            config_map['logging']['logfile']
        except Exception:
            sys.exit('Error: Logging option "logfile" has not been set.')

        # MySQL
        try:
            config_map['mysql']['host']
        except Exception:
            sys.exit('Error: MySQL option "host" has not been set.')
        try:
            config_map['mysql']['port']
        except Exception:
            config_map['mysql']['port'] = 3306
        try:
            config_map['mysql']['port'] = int(config_map['mysql']['port'])
        except Exception:
            sys.exit('Error: MySQL option "port" is invalid.')
        try:
            config_map['mysql']['username']
        except Exception:
            sys.exit('Error: MySQL option "username" has not been set.')
        try:
            config_map['mysql']['password']
        except Exception:
            sys.exit('Error: MySQL option "password" has not been set.')
        try:
            config_map['mysql']['database']
        except Exception:
            sys.exit('Error: MySQL option "database" has not been set.')

        # Postgres
        try:
            config_map['postgres']['host']
        except Exception:
            sys.exit('Error: Postgres option "host" has not been set.')
        try:
            config_map['postgres']['port']
        except Exception:
            config_map['postgres']['port'] = 5432
        try:
            config_map['postgres']['port'] = int(config_map['postgres']['port'])
        except Exception:
            sys.exit('Error: Postgres option "port" is invalid.')
        try:
            config_map['postgres']['username']
        except Exception:
            sys.exit('Error: Postgres option "username" has not been set.')
        try:
            config_map['postgres']['password']
        except Exception:
            sys.exit('Error: Postgres option "password" has not been set.')
        try:
            config_map['postgres']['database']
        except Exception:
            sys.exit('Error: Postgres option "database" has not been set.')

        # Sensors
        try:
            config_map['sensors']['ping_intervall']
        except Exception:
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
    def postgres(self):
        """
        Returns the part of the configuration containing the Postgres config
        """
        return self.__options['postgres']

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
