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

from abc import ABCMeta, abstractmethod, abstractproperty
from sensors.tinkerforge.ip_connection import Error as IPConError
import time

class Sensor(object):
    """
    Base class for all Tinkerforge API wrappers
    """
    __metaclass__ = ABCMeta

    @property
    def uid(self):
        """
        Returns the UID of the bricklet
        """
        return self.__uid

    def callback(self, value):
        """
        Checks all values before sending them to the sensor host. If a value was returned too early, that means before the callback period is over,
        the value will be discarded. This is most probably due to another process like the BrickViewer registering its own callback with the bricklet.
        """
        last_update = self.last_update

        # If the callback was too soon.
        # This takes care of diverging clocks in each device.
        if ( last_update is None or last_update >= self.callback_period * 0.9 ):
            self.last_update = time.time()
            self.__callback_method(self, float(value), last_update)
        else:
            self.check_callback()
            self.logger.warning('Warning. Discarding value "%s" from sensor "%s", because the last update was too recent. Was %i s ago, but supposed to be every %i s.', value, self.uid, last_update / 1000, self.callback_period / 1000)

    def check_callback(self):
        """
        Check whether the callback ist still properly registered. A bricklet can only manage one registered callback. So make sure this is ours!
        Returns true if the sensor was found else false.
        """
        try: 
            current_period = self.sensor_callback_period
            if current_period != self.callback_period:
                self.logger.warning('Warning. Callback for sensor "%s" is set to %d ms instead of %d ms. Resetting.', self.uid, current_period, self.callback_period)
                self.set_callback()
            return True
        except IPConError as e:
            if (e.value == IPConError.TIMEOUT):
                self.logger.warning('Warning. Lost connection to sensor "%s".', self.uid)
            else:
                raise
        return False

    def disconnect(self):
        """
        Disable the callback on the bricklet.
        """
        self.logger.debug('Disconnecting sensor "%s" from host "%s".', self.uid, self.parent.host_name)
        self.__callback_period = 0
        self.set_callback()

    @abstractproperty
    def unit(self):
        """
        Returns the SI unit of the measurand.
        """
        pass

    @abstractproperty
    def bricklet(self):
        """
        Returns the Tinkerforge API bricklet object
        """
        pass

    @abstractproperty
    def sensor_type(self):
        """
        Return the type of mesurand beeing measured.
        """
        pass

    @abstractmethod
    def set_callback(self):
        """
        Sets the callback period and registers the method callback() with the Tinkerforge API.
        """
        pass

    @property
    def parent(self):
        """
        Returns the SensorHost object to which the sensor is connected
        """
        return self.__parent

    @property
    def logger(self):
        """
        Returns the return the logger object for this sensor
        """
        return self.parent.logger

    @property
    def last_update(self):
        """
        Returns the timedelta between now and the last update registered in ms.
        Return None if there was no update so far.
        """
        if (self.__last_update is not None):
            return int((time.time() - self.__last_update) * 1000)
        else:
            return None

    @last_update.setter
    def last_update(self, value):
        """
        Sets the time in seconds since the epoch as a floating point number.
        Set to None to if there was no update so far.
        """
        self.__last_update = value

    @property
    def callback_period(self):
        """
        Returns the callback period in ms.
        """
        return self.__callback_period

    @abstractproperty
    def sensor_callback_period(self):
        """
        Returns the callback period in ms.
        """
        pass

    def __init__(self, uid, parent, callback_method, callback_period=0):
        """
        Create new sensor Object.
        uid: UID of the sensor
        parent: The SensorHost object to which the sensor is connected
        callbackMethod: The SensorHost callback to deliver to the data to.
        callbackPeriod: The callback period in ms. A value of 0 will turn off the callback.
        """
        if callback_period == 0:
            parent.logger.debug('Disabling %s sensor "%s". No callback period set.', self.sensor_type, uid, callback_period)
        else:
            parent.logger.debug('Adding %s sensor "%s" with callback period %d ms.', self.sensor_type, uid, callback_period)

        self.__uid = uid
        self.__parent = parent
        self.__callback_method = callback_method
        self.__callback_period = callback_period
        self.__last_update = None
