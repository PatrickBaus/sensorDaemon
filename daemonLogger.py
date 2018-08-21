# -*- coding: utf-8 -*-
# ##### BEGIN GPL LICENSE BLOCK #####
#
# Copyright (C) 2015  Patrick Baus
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

import datetime as dt
import logging
import sys
import traceback

class _LinuxColoredFormatter(logging.Formatter):
    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
    # See http://jafrog.com/2013/11/23/colors-in-terminal.html for an explanation of the codes
    RESET_SEQ = "\033[0m"
    COLOR_SEQ = "\033[1;%dm"
    BOLD_SEQ = "\033[1m"

    COLORS = {
       'WARNING': YELLOW,
       'INFO': WHITE,
       'DEBUG': BLUE,
       'CRITICAL': RED,
       'ERROR': RED
    }

    converter=dt.datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
       ct = self.converter(record.created)
       if datefmt:
           s = ct.strftime(datefmt)
       else:
           t = ct.strftime("%Y-%m-%d %H:%M:%S")
           s = "%s,%03d" % (t, record.msecs)
       return s

    def format(self, record):
       levelname = record.levelname
       text = super(ColoredFormatter, self).format(record)
       if levelname in ColoredFormatter.COLORS:
          text = self.COLOR_SEQ % (30 + self.COLORS[levelname]) + text + self.RESET_SEQ
       return text

class _WindowsColoredFormatter(logging.Formatter):
    # wincon.h
    FOREGROUND_BLACK     = 0x0000
    FOREGROUND_BLUE      = 0x0001
    FOREGROUND_GREEN     = 0x0002
    FOREGROUND_CYAN      = 0x0003
    FOREGROUND_RED       = 0x0004
    FOREGROUND_MAGENTA   = 0x0005
    FOREGROUND_YELLOW    = 0x0006
    FOREGROUND_GREY      = 0x0007
    FOREGROUND_INTENSITY = 0x0008 # foreground color is intensified.
    FOREGROUND_WHITE     = FOREGROUND_BLUE | FOREGROUND_GREEN | FOREGROUND_RED

    BACKGROUND_BLACK     = 0x0000
    BACKGROUND_BLUE      = 0x0010
    BACKGROUND_GREEN     = 0x0020
    BACKGROUND_CYAN      = 0x0030
    BACKGROUND_RED       = 0x0040
    BACKGROUND_MAGENTA   = 0x0050
    BACKGROUND_YELLOW    = 0x0060
    BACKGROUND_GREY      = 0x0070
    BACKGROUND_INTENSITY = 0x0080 # background color is intensified.

    COLORS = {
       'WARNING': FOREGROUND_YELLOW | FOREGROUND_INTENSITY,
       'INFO': FOREGROUND_WHITE,
       'DEBUG': FOREGROUND_BLUE,
       'CRITICAL': BACKGROUND_YELLOW | FOREGROUND_RED | FOREGROUND_INTENSITY | BACKGROUND_INTENSITY,
       'ERROR': FOREGROUND_RED | FOREGROUND_INTENSITY
    }

# select ColorStreamHandler based on platform
import platform
if platform.system() == 'Windows':
   ColoredFormatter = _WindowsColoredFormatter
else:
   ColoredFormatter = _LinuxColoredFormatter

class DaemonLogger(object):
    """
    This class manages the loggers for the daemon. Since Python does not instantiate loggers directly
    no inheritance will be used. See: http://docs.python.org/2/library/logging.html#logger-objects
    """
    def uncaught_exception_handler(self,type, value, tb):
        """
        Uncaught excpetion handler
        """
        self.get_logger().critical(''.join(traceback.format_tb(tb)))
        self.get_logger().critical('{0}: {1}'.format(type, value))

    def __init__(self, config):
        """
        Creates a daemonLogger object.
        config: A dictionary that contains a relevant configuration options for the logger
        """

        """
        Log levels:
        CRITICAL 50
        ERROR    40
        WARNING  30
        INFO     20
        DEBUG    10
        NOTSET   0
        """
        log_levels_used = [config['console_loglevel'], config['file_loglevel']]
        # Find the minium log level required. Since NOTSET is 0, we exclude it from the list, but it the list is empty
        # we know that it is not set, hence the default. The default keyword requires python 3.4
        self.get_logger().setLevel(min((level for level in log_levels_used if level is not logging.NOTSET), default=logging.NOTSET))

        # Create console logger only if the daemon is run from terminal
        if config['console_loglevel'] is not logging.NOTSET:
            console_logger = logging.StreamHandler()
            console_logger.setLevel(config['console_loglevel'])
            console_logger.setFormatter(ColoredFormatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
            self.get_logger().addHandler(console_logger)

        # Create file logger for loglevel >= config['file_loglevel']
        if config['file_loglevel'] is not logging.NOTSET:
          file_logger = logging.FileHandler(config['logfile'])
          file_logger.setLevel(config['file_loglevel'])
          formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt=config['dateformat'])
          file_logger.setFormatter(formatter)
          self.get_logger().addHandler(file_logger)

        # Set uncaught exception handler to log those exception to file as well
        sys.excepthook = self.uncaught_exception_handler

    def get_logger(self):
        """
        Returns the logger object. The first call will create the logger.
        """
        return logging.getLogger('SensorDaemon')

    def shutdown(self):
        """
        Shutdown the logging system. This function should be called before closing down the daemon.
        """
        return logging.shutdown()
