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

import asyncio
import logging

from tinkerforge_async.ip_connection import EnumerationType, NotConnectedError, IPConnectionAsync as IPConnection
from tinkerforge_async.devices import DeviceIdentifier
from tinkerforge_async.device_factory import device_factory

from .tinkerforge import TinkerforgeSensor
from .sensor_host import SensorHost

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

    def __init__(self, hostname, port, config, parent):
        """
        Create new sensorHost Object.
        hostName: IP or hostname of the machine hosting the sensor daemon
        port: port on the host
        parent: sensorDaemon object managing all sensor hosts
        """
        super().__init__(hostname, port, config, parent)
        self.__logger = logging.getLogger(__name__)
        self.__conn = IPConnection()
        self.__failed_connection_attemps = 0
        self.__running_tasks = []
        self.__sensors = {}

    def __repr__(self):
        return f'{self.__class__.__module__}.{self.__class__.__qualname__}(hostname={self.hostname}, port={self.port}, config={self.config}, parent={self.parent!r})'

    def sensor_callback(self, sensor, value, previous_update):
        """
        The callback used by all sensors to send their data to the sensor daemon.
        """
        self.parent.host_callback(sensor.sensor_type, sensor.uid, value, previous_update)

    def get_sensor_config(self, uid):
        return self.parent.get_sensor_config(uid)

    async def process_sensor_data(self, sensor):
        async for event in sensor.read_events():
            await self.process_value(sensor.uid, event['sid'], event['payload'])
        del self.__sensors[sensor.uid]

    async def process_enumerations(self):
        """
        This infinite loop pulls events from the internal enumeration queue
        of the ip connection and waits for an enumeration event to create the devices.
        """
        try:
            async for packet in self.__conn.read_enumeration():
                if packet["enumeration_type"] in (EnumerationType.CONNECTED, EnumerationType.AVAILABLE):
                    async for sensor_config in self.get_sensor_config(packet["uid"]):
                        if sensor_config is not None:
                            try:
                                if packet["uid"] not in self.__sensors:
                                    device = TinkerforgeSensor(packet["device_id"], packet["uid"], self.__conn, self)
                                    await device.run()
                                    self.__sensors[device.uid] = (device, asyncio.create_task(self.process_sensor_data(device), name='sensor-'+str(device.uid)))
                            except ValueError:
                                # raised by the sensor factory if a device_id cannot be found
                                self.__logger.warning("Unsupported device id '%s' with uid '%s' found on host '%s'", packet["uid"], packet["device_id"], self.hostname)
                            except Exception:
                                self.__logger.exception("Error during sensor init.")
                elif packet["enumeration_type"] is EnumerationType.DISCONNECTED:
                    # Check whether the sensor was actually connected to this host, then remove it.
                    try:
                        _, device_task = self.__sensors.pop(packet["uid"])
                        device_task.cancel()
                        try:
                            await device_task
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            self.__logger.exception("Error while removing sensor '%s' from host '%s'", packet["uid"], self.hostname)
                    except KeyError:
                        pass    # Do nothing if the device is not registered
        except NotConnectedError:
            self.__logger.debug("Cannot process enumerations. Waiting for connection to host '%s%i'", self.hostname, self.port)

    async def connect(self):
        """
        Start up the ip connection
        """
        # TODO: Add incremental delay after connection failure
        if self.failed_connection_attemps == 0:
            self.__logger.warning("Connecting to brick daemon on '%s:%i'...", self.hostname, self.port)
        try:
            task = asyncio.create_task(self.__conn.connect(host=self.hostname, port=self.port))
            self.__running_tasks.append(task)
            try:
                await task
            finally:
                self.__running_tasks.remove(task)
            self.__logger.info("Connected to host '%s:%i'.", self.hostname, self.port)
        except (OSError, asyncio.TimeoutError) as e:
            self.__failed_connection_attemps += 1
            # Suppress the warning after __MAXIMUM_FAILED_CONNECTIONS to stop spamming log files
            if (self.failed_connection_attemps < self.__MAXIMUM_FAILED_CONNECTIONS):
                if self.failed_connection_attemps > 1:
                    failure_count = " (%d times)" % self.failed_connection_attemps
                else:
                    failure_count = ""
                self.__logger.warning("Failed to connect to host '%s'%s. Error: %s.", self.hostname, failure_count, e)
            if (self.failed_connection_attemps == self.__MAXIMUM_FAILED_CONNECTIONS):
                self.__logger.warning("Failed to connect to host '%s' (%d time%s). Error: %s. Suppressing warnings from hereon.", self.hostname, self.failed_connection_attemps, "s"[self.failed_connection_attemps == 1:], e)

        if self.is_connected:
            self.__failed_connection_attemps = 0

    @property
    def is_connected(self):
        """
        Checks the state of the ip connection. Will return false if the device is no longer connected.
        """
        return self.__conn.is_connected

    async def disconnect(self):
        """
        Disconnect all sensors from the host. This will disable the callbacks of the sensors.
        """
        tasks = [sensor.disconnect() for sensor, _ in self.__sensors.values()]
        await asyncio.gather(*tasks)

        await self.__conn.disconnect()

        tasks = [task for task in self.__running_tasks]
        [task.cancel() for task in tasks]
        # TODO: Maybe use return_exceptions=True
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        except Exception:
            self.__logger.exception("Exception during disconnect of host %s:%i", self.hostname, self.port)
        self.__running_tasks.clear()

    async def connection_watchdog(self):
        try:
            while "loop not canceled":
                if not self.is_connected:
                    await self.connect()
                    if self.is_connected:
                        # Start the enumeration callback task
                        enumeration_task = asyncio.create_task(self.process_enumerations())
                        self.__running_tasks.append(enumeration_task)
                        # Enumerate all sensors
                        await self.__conn.enumerate()
                        await enumeration_task  # This will return on disconnect
                    else:
                        # If we cannot connect, stay a while and listen
                        await asyncio.sleep(5)    # TODO: make the timeout configurable
        finally:
            self.__logger.debug("Shutting down sensor host %s:%i", self.hostname, self.port)

    async def run(self):
        task = asyncio.create_task(self.connection_watchdog())
        self.__running_tasks.append(task)
        await task
