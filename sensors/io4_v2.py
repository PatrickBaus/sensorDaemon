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

from .sensor import Sensor
from .tinkerforge.bricklet_io4_v2 import BrickletIO4V2 as Bricklet


class BrickletIO4(Sensor):
    """
    API Wrapper for the Tinkerforge IO-4 v2.0 bricklet
    """
    UNIT = None
    # The type will be used for describing the sensor, like "Registering %TYPE sensor."
    TYPE = "digital i/o"
    DEVICE_IDENTIFIER = Bricklet.DEVICE_IDENTIFIER

    @property
    def unit(self):
        """
        Returns the SI unit of the measurand.
        """
        return self.UNIT

    @property
    def sensor_type(self):
        """
        Return the type of mesurand beeing measured.
        """
        return self.TYPE

    @property
    def sensor_callback_period(self):
        """
        Returns the callback period in ms.
        """
        return self.bricklet.get_input_value_callback_configuration(channel)

    @property
    def bricklet(self):
        """
        Returns the Tinkerforge API Bricklet object
        """
        return self.__bricklet

    def callback(self, channel, changed, value):
        """
        This method will be called by the API, when a new value is available.
        It does all the conversion to the apropriate SI unit specified by getUnit().
        value: the value as returned by the bricklet. This might not be in SI units.
        """
        super().callback(value, channel)

    def set_callback(self):
        """
        Sets the callback period and registers the method callback() with the Tinkerforge API.
        """
        for channel in range(4):
            self.bricklet.set_input_value_callback_configuration(channel=channel, period=self.callback_period, value_has_to_change=False)
        self.bricklet.register_callback(self.bricklet.CALLBACK_INPUT_VALUE, self.callback)

    def __init__(self, uid, parent, callback_method, callback_period=0):
        """
        Create new sensor Object.
        uid: UID of the sensor
        parent: The SensorHost object to which the sensor is connected
        callbackMethod: The SensorHost callback to deliver to the data to.
        callbackPeriod: The callback period in ms. A value of 0 will turn off the callback.
        """
        super().__init__(uid, parent, callback_method, callback_period)

        self.__bricklet = Bricklet(uid, parent.ipcon)
        for channel in range(4):
            self.bricklet.set_configuration(channel, self.bricklet.DIRECTION_IN, True)
        self.set_callback()
