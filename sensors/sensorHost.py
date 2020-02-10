## -*- coding: utf-8 -*-
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

from __future__ import absolute_import, division, print_function
from builtins import dict

from socket import error as socketError

from .tinkerforge.ip_connection import IPConnection
from .tinkerforge.ip_connection import Error as IPConError
from .ambientLight import AmbientLightSensor
from .ambientLight_v2 import AmbientLightSensorV2
from .barometer import BarometerSensor
from .humidity import HumiditySensor
from .humidity_v2 import HumiditySensorV2
from .temperature import TemperatureSensor
from .ptc import PTCSensor

class SensorHost(object):
    """
    Class that wraps a Tinkerforge brick daemon host. This can be either a PC running brickd, or a master brick with an ethernet/wifi ectension
    """
    __MAXIMUM_FAILED_CONNECTIONS = 3
    @property
    def failed_connection_attemps(self):
        """
        Returns the number of failed connection attemps.
        """
        return self.__failed_connection_attemps

    @property
    def host_name(self):
        """
        Returns the hostname of the Tinkerforge brick daemon, where this host can be found
        """
        return self.__host_name

    @property
    def port(self):
        """
        Returns the port at which the Tinkerforge brick daemon is listening
        """
        return self.__port

    @property
    def sensors(self):
        """
        Returns a dictionary of all sensors registered with this host. getSensors()[%sensor_uid] will return a sensor object.
        """
        return self.__sensors

    @property
    def logger(self):
        """
        Returns the logger to send messages to the console/file
        """
        return self.parent.logger

    @property
    def parent(self):
        """
        Returns the sensor daemon object.
        """
        return self.__parent

    def append_sensor(self, sensor):
        """
        Appends a new sensor to the host.
        """
        self.sensors[sensor.uid] = sensor
        
    def remove_sensor(self, sensor):
        """
        Removes a sensor from the host. This will be done gracefully.
        """
        try:
            sensor.disconnect()
        except IPConError as e:
            if (e.value == IPConError.TIMEOUT):
                self.logger.warning('Warning. Cannot disconnect from sensor "%s" on host "%s". The sensor or host does not respond. Will now forcefully remove the sensor.', sensor.uid, self.host_name)
                del self.sensors[sensor.uid]

    @property
    def ipcon(self):
        """
        Returns the Tinkerforge IP Connection object.
        http://www.tinkerforge.com/en/doc/Software/IPConnection_Python.html
        """
        return self.__ipcon

    def __enumeration_id_to_string(self, id):
        """
        Returns a human readable string when given an IPConnection enum.
        """
        result = "Unknown"
        if id == IPConnection.ENUMERATION_TYPE_AVAILABLE:
            result = 'User request'
        elif id == IPConnection.ENUMERATION_TYPE_CONNECTED:
            result = 'Device connected'
        elif id == IPConnection.ENUMERATION_TYPE_DISCONNECTED:
            result = 'Device discconnected from host'
        return result
        
    def __connect_sensor(self, sensor_class, uid, connected_uid, port, name, enumeration_type):
        self.logger.warning('%s sensor "%s" connected to brick "%s" (Port: "%s") on host "%s". Reason: %s (%i).', sensor_class.TYPE.title(), uid, connected_uid, port, name, self.__enumeration_id_to_string(enumeration_type), enumeration_type)
        # Retrieve the callback period from the database
        callback_period = self.parent.get_callback_period(uid)
        # Create new sensor obect and set up callbacks
        if (callback_period > 0):
            self.append_sensor(sensor_class(uid, self, self.sensor_callback, callback_period))

    def ping(self):
        """
        Check the connection to the brick daemon and subsequently all sensors connected to a host.
        """
        self.logger.debug('Pinging host "%s"', self.host_name)
        if self.is_connected:
            self.logger.debug('Host "%s" seems connected. Checking %i sensors.', self.host_name, len(self.sensors))
            if len(self.sensors) > 0:
                try:
                    # Remove the sensor if it cannot be found. Copy
                    # those sensors to a list first, because we can't remove any sensors
                    # from the dict while iterating over it.
                    offline_sensors = []
                    for sensor in self.sensors.values():
                        self.logger.debug('Checking sensor "%s".', sensor.uid)
                        if not sensor.check_callback():
                            offline_sensors.append(sensor)
                    for sensor in offline_sensors:
                        self.remove_sensor(sensor)

                except IPConError as e:
                    if (e.value == IPConError.TIMEOUT):
                        self.logger.error('Error. Lost connection to host "%s".', self.host_name)
                    else:
                        raise
        else:
            self.logger.debug('Host "%s" seems disconnected. Trying to reconnect.', self.host_name)
            self.connect()

    def sensor_callback(self, sensor, value, previous_update):
        """
        The callback used by all sensors to send their data to the sensor daemon.
        """
        self.parent.host_callback(sensor.sensor_type, sensor.uid, value, previous_update)

    def connect_callback(self, connected_reason):
        """
        This callback is used by the IPConnection object to communicate any chanes in the connection state to the brick daemon.
        """
        if self.ipcon.get_connection_state() == IPConnection.CONNECTION_STATE_CONNECTED:
            # Remove all current sensors from this node, then enumerate
            self.logger.warning('Enumerating devices on host "%s". Reason: %s (%i).', self.host_name, self.__enumeration_id_to_string(connected_reason), connected_reason)
            self.disconnect_sensors()
            self.ipcon.enumerate()
        else:
            self.logger.error('Cannot enumerate host "%s". Host not connected. Reason: %s (%i).', self.host_name, self.__enumeration_id_to_string(connected_reason), connected_reason)

    def enumerate_callback(self, uid, connected_uid, position, hardware_version, firmware_version, device_identifier, enumeration_type):
        """
        This method does all the work. Every sensor connector to a brick will return its type, uid (and more) on an enumerate call. We will then
        create sensor objects according to the type of sensor answering the call.
        Disconnecting sensors will also be treated here.
        For details see: http://www.tinkerforge.com/en/doc/Software/IPConnection_Python.html#IPConnection.CALLBACK_ENUMERATE
        """
        if enumeration_type == IPConnection.ENUMERATION_TYPE_CONNECTED or enumeration_type == IPConnection.ENUMERATION_TYPE_AVAILABLE:
            # Ambient light bricklet
            if device_identifier == AmbientLightSensor.DEVICE_IDENTIFIER:
                self.__connect_sensor(AmbientLightSensor, uid, connected_uid, position, self.host_name, enumeration_type)
            # Ambient light bricklet v2
            if device_identifier == AmbientLightSensorV2.DEVICE_IDENTIFIER:
                self.__connect_sensor(AmbientLightSensorV2, uid, connected_uid, position, self.host_name, enumeration_type)
            # Barometer bricklet
            elif device_identifier == BarometerSensor.DEVICE_IDENTIFIER:
                self.__connect_sensor(BarometerSensor, uid, connected_uid, position, self.host_name, enumeration_type)
            # Humidity bricklet
            elif device_identifier == HumiditySensor.DEVICE_IDENTIFIER:
                self.__connect_sensor(HumiditySensor, uid, connected_uid, position, self.host_name, enumeration_type)
            # Humidity bricklet v2
            elif device_identifier == HumiditySensorV2.DEVICE_IDENTIFIER:
                self.__connect_sensor(HumiditySensorV2, uid, connected_uid, position, self.host_name, enumeration_type)
            # Temperature bricklet
            elif device_identifier == TemperatureSensor.DEVICE_IDENTIFIER:
                self.__connect_sensor(TemperatureSensor, uid, connected_uid, position, self.host_name, enumeration_type)
            # PTC bricklet
            elif device_identifier == PTCSensor.DEVICE_IDENTIFIER:
                self.__connect_sensor(PTCSensor, uid, connected_uid, position, self.host_name, enumeration_type)
            # Device is not a bricklet
            elif device_identifier < 128:
                pass
            # Throw an error if the device is not supported.
            else:
                self.logger.error('Unsupported device (uid "%s", identifier "%s") found on host "%s"', uid, device_identifier, self.host_name)

        elif enumeration_type == IPConnection.ENUMERATION_TYPE_DISCONNECTED:
            # Check whether the sensor is actually connected to this host and if it is an actual sensor and not a master brick.
            # If the sensor is connected to this node, delete it.
            if uid in self.sensors:
                self.logger.warning('Sensor "%s" disconnected from host "%s".', uid, self.host_name)
                del self.sensors[uid]
        else:
            self.logger.debug('Recieved an unknown enumeration type: %s', enumeration_type)

    def connect(self):
        """
        Start up the ip connection to the various brick daemons
        """
        try:
            self.ipcon.connect(self.host_name, self.port)
            self.logger.info('Connected to host "%s".', self.host_name)
        except socketError as e:
            self.__failed_connection_attemps += 1
            # Suppress the warning after __MAXIMUM_FAILED_CONNECTIONS to stop spamming log files
            if (self.failed_connection_attemps < self.__MAXIMUM_FAILED_CONNECTIONS):
                if self.failed_connection_attemps > 1:
                    failure_count = " (%d times)" % self.failed_connection_attemps
                else:
                    failure_count = ""
                self.logger.warning('Warning. Failed to connect to host "%s"%s. Error: %s.', self.host_name, failure_count, e)
            if (self.failed_connection_attemps == self.__MAXIMUM_FAILED_CONNECTIONS):
                self.logger.warning('Warning. Failed to connect to host "%s" (%d time%s). Error: %s. Suppressing warnings from hereon.', self.host_name, self.failed_connection_attemps, "s"[self.failed_connection_attemps==1:], e)

        if self.is_connected:
           self.__failed_connection_attemps = 0

    @property
    def is_connected(self):
        """
        Checks the state of the ip connection. Will return false if the device is no longer connected.
        """
        return self.ipcon.get_connection_state() == IPConnection.CONNECTION_STATE_CONNECTED

    def disconnect_sensors(self):
        """
        Disconnect and remove all sensors from this node.
        """
        for uid in self.sensors.keys():
            self.remove_sensor(self.sensors[uid])

    def disconnect(self):
        """
        Disconnect all sensors from the host. This will disable the callbacks of the sensors.
        """
        if self.is_connected:
            self.logger.warning('Disconnecting from brick daemon on "%s"...', self.host_name)
            self.disconnect_sensors()
            try:
                self.ipcon.disconnect()
                pass
            except socketError as e:
                self.logger.warning('Warning. Failed to disconnect from host "%s". Error: %s.', self.host_name, e)

    def __init__(self, host_name, port, parent):
        """
        Create new sensorHost Object.
        hostName: IP or hostname of the machine hosting the sensor daemon
        port: port on the host
        parent: sensorDaemon object managing all sensor hosts
        """
        self.__host_name = host_name
        self.__port = port
        self.__parent = parent
        self.__sensors = {}
        self.__ipcon = IPConnection()
        self.__ipcon.register_callback(IPConnection.CALLBACK_ENUMERATE, self.enumerate_callback)
        self.__ipcon.register_callback(IPConnection.CALLBACK_CONNECTED, self.connect_callback)
        self.__failed_connection_attemps = 0
