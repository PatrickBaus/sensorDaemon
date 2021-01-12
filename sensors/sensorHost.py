## -*- coding: utf-8 -*-
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

from abc import ABCMeta, abstractmethod
import asyncio
import logging
from socket import error as socketError

from .tinkerforgeAsync.source.ip_connection import EnumerationType, IPConnectionAsync as IPConnection
from .tinkerforgeAsync.source.devices import device_factory
from .tinkerforge.ip_connection import Error as IPConError

class SensorHostFactory:
    def __init__(self):
        self.__available_hosts= {}

    def register(self, driver, host):
        self.__available_hosts[driver] = host

    def get(self, driver, hostname, port, parent):
        host = self.__available_hosts.get(driver)
        if host is None:
            raise ValueError(f"No driver available for {driver}")
        return host(hostname, port, parent)

class SensorHost(metaclass=ABCMeta):

    @property
    def hostname(self):
        """
        Returns the hostname of the Tinkerforge brick daemon, where this host can be found
        """
        return self.__hostname

    @property
    def port(self):
        """
        Returns the port at which the Tinkerforge brick daemon is listening
        """
        return self.__port

    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def disconnect(self):
        pass

    def __init__(self, hostname, port, parent):
        self.__hostname = hostname
        self.__port = port
        self.__parent = parent

class TinkerforgeSensorHost(SensorHost):
    """
    Class that wraps a Tinkerforge brick daemon host. This can be either a PC running brickd, or a master brick with an ethernet/wifi extension
    """
    __MAXIMUM_FAILED_CONNECTIONS = 3
    @property
    def failed_connection_attemps(self):
        """
        Returns the number of failed connection attemps.
        """
        return self.__failed_connection_attemps

    @property
    def sensors(self):
        """
        Returns a dictionary of all sensors registered with this host. getSensors()[%sensor_uid] will return a sensor object.
        """
        return self.__sensors

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
                self.logger.warning('Warning. Cannot disconnect from sensor "%s" on host "%s". The sensor or host does not respond. Will now forcefully remove the sensor.', sensor.uid, self.__hostname)
                if sensor.uid in self.sensors:
                    # If the sensor was disconnected by hand before we could remove it, it might already be gone
                    del self.sensors[sensor.uid]

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
        self.logger.debug('Pinging host "%s"', self.__hostname)
        if self.is_connected:
            self.logger.debug('Host "%s" seems connected. Checking %i sensors.', self.__hostname, len(self.sensors))
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
                        self.logger.error('Error. Lost connection to host "%s".', self.__hostname)
                    else:
                        raise
        else:
            self.logger.debug('Host "%s" seems disconnected. Trying to reconnect.', self.__hostname)
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
            self.logger.warning('Enumerating devices on host "%s". Reason: %s (%i).', self.__hostname, self.__enumeration_id_to_string(connected_reason), connected_reason)
            self.disconnect_sensors()
            self.ipcon.enumerate()
        else:
            self.logger.error('Cannot enumerate host "%s". Host not connected. Reason: %s (%i).', self.__hostname, self.__enumeration_id_to_string(connected_reason), connected_reason)

    async def process_enumerations():
        """
        This infinite loop pulls events from the internal enumeration queue
        of the ip connection and waits for an enumeration event to create the devices
        """
        while "queue not canceled":
            packet = await self.__conn.enumeration_queue.get()
            if enumeration_type is EnumerationType.CONNECTED or enumeration_type is EnumerationType.AVAILABLE:
                try:
                    device = device_factory(packet["device_id"], packet["uid"], self.__conn)
                except ValueError:
                    self.logger.warning("Unsupported device (uid '%s', identifier '%s') found on host '%s'", uid, device_identifier, self.hostname)
            elif enumeration_type is EnumerationType.DICCONNECTED:
                # Check whether the sensor is actually connected to this host, then remove it.
                if uid in self.sensors:
                    self.logger.warning("Sensor '%s' disconnected from host '%s'.", uid, self.hostname)
                    del self.sensors[uid]

    async def connect(self):
        """
        Start up the ip connection
        """
        if self.failed_connection_attemps == 0:
            self.__logger.warning("Connecting to brick daemon on '%s'...", self.hostname)
        try:
            await self.__conn.connect(self.hostname, self.port)
            self.__logger.info("Connected to host '%s'.", self.hostname)
        except socketError as e:
            self.__failed_connection_attemps += 1
            # Suppress the warning after __MAXIMUM_FAILED_CONNECTIONS to stop spamming log files
            if (self.failed_connection_attemps < self.__MAXIMUM_FAILED_CONNECTIONS):
                if self.failed_connection_attemps > 1:
                    failure_count = " (%d times)" % self.failed_connection_attemps
                else:
                    failure_count = ""
                self.__logger.warning("Failed to connect to host '%s'%s. Error: %s.", self.hostname, failure_count, e)
            if (self.failed_connection_attemps == self.__MAXIMUM_FAILED_CONNECTIONS):
                self.__logger.warning("Failed to connect to host '%s' (%d time%s). Error: %s. Suppressing warnings from hereon.", self.hostname, self.failed_connection_attemps, "s"[self.failed_connection_attemps==1:], e)

        if self.is_connected:
           self.__failed_connection_attemps = 0

    @property
    def is_connected(self):
        """
        Checks the state of the ip connection. Will return false if the device is no longer connected.
        """
        return self.__conn.is_connected

    def disconnect_sensors(self):
        """
        Disconnect and remove all sensors from this node.
        """
        for uid in list(self.sensors):
            self.remove_sensor(self.sensors[uid])

    async def disconnect(self):
        """
        Disconnect all sensors from the host. This will disable the callbacks of the sensors.
        """
        if self.is_connected:
            await self.__conn.disconnect()

    async def run(self):
        try:
          self.__running_tasks.append(asyncio.create_task(self.process_enumerations()))

            # TODO: Catch connection error
            while not self.is_connected:
                await self.connect()

            await self.__conn.enumerate()

            while "loop not canceled":
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            await self.disconnect()
            raise

    def __init__(self, hostname, port, parent):
        """
        Create new sensorHost Object.
        hostName: IP or hostname of the machine hosting the sensor daemon
        port: port on the host
        parent: sensorDaemon object managing all sensor hosts
        """
        super().__init__(hostname, port, parent)
        self.__logger = logging.getLogger(__name__)
        self.__sensors = {}
        self.__conn = IPConnection()
        self.__running_tasks = []
        self.__failed_connection_attemps = 0

host_factory = SensorHostFactory()
host_factory.register(driver="tinkerforge", host=TinkerforgeSensorHost)
