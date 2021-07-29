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
from abc import ABCMeta, abstractmethod, abstractproperty
import time

from tinkerforge_async.devices import GetCallbackConfiguration, ThresholdOption
from tinkerforge_async.device_factory import device_factory


class TinkerforgeSensor():
    """
    Base class for all Tinkerforge Sensors
    """
    @property
    def uid(self):
        return self.__sensor.uid

    def __init__(self, device_id, uid, ipcon, parent):
        self.__sensor = device_factory.get(device_id, uid, ipcon)
        self.__parent = parent
        self.__config = None
        self.__logger = logging.getLogger(__name__)
        self.__callback_config = {}
        self.__running_tasks = []

    def __str__(self):
        return f'Tinkerforge {str(self.__sensor)}'

    def __repr__(self):
        return f'{self.__class__.__module__}.{self.__class__.__qualname__}(device_id={self.__sensor.DEVICE_IDENTIFIER!r}, uid={self.__sensor.uid}, ipcon={self.__sensor.ipcon!r})'

    async def __init_sensor(self, config):
        on_connect = config.get("on_connect")
        if on_connect is not None:
            for cmd in on_connect:
                try:
                    function = getattr(self.__sensor, cmd["function"])
                except AttributeError:
                    self.__logger.error("Invalid config parameter '%s' for sensor %s", cmd["function"], self.__sensor)
                    continue

                try:
                    result = function(*cmd.get("args", []), **cmd.get("kwargs", {}))
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    self.__logger.exception("Error running config for sensor %s on host '%s'", self.__sensor.uid, self.__parent.hostname)
                    continue

        # Enable the callback
        self.__callback_config[config['sid']] = GetCallbackConfiguration(
            period=config['period'],
            value_has_to_change=False,
            option=ThresholdOption.OFF,
            minimum=0,
            maximum=0
        )
        await self.__register_callback(config['sid'], self.__callback_config[config['sid']])

    async def __register_callback(self, sid, config):
        await self.__sensor.set_callback_configuration(sid, *config)
        self.__sensor.register_event(sid=sid)

    async def __test_callback(self, sid):
        config = await self.__sensor.get_callback_configuration(sid)
        if config != self.__callback_config[sid]:
            await self.__sensor.set_callback_configuration(sid, *self.__callback_config)

    async def read_events(self):
        last_update = {sid: time.time() - self.__callback_config[sid].period / 1000 for sid in self.__callback_config.keys()}
        check_callback_job = None
        async for event in self.__sensor.read_events():
            sid = event['sid']
            if (event['timestamp'] - last_update[sid] <= 0.9 * self.__callback_config[sid].period / 1000):
                self.__logger.warning(f"Callback {event} was {self.__callback_config[sid].period / 1000 - float(data['timestamp'] - last_update[sid])} ms too soon. Callback is set to {self.__callback_config[sid].period} ms.")
                if check_callback_job is None or check_callback_job.done():
                    # Schedule a test of the callback config with background job
                    # manager. It will take care of cleanup as well.
                    # Skip the test if we have a test scheduled already
                    check_callback_job = asyncio.create_task(self.__test_callback(sid))
                    await self.__background_job_queue.put(check_callback_job)
            last_update[sid] = event['timestamp']
            yield event

    async def __background_job_manager(self):
        while "loop not canceled":
            job = await self.__background_job_queue.get()
            try:
                await job
            except asyncio.CancelledError:
                raise
            except Exception:
                self.__logger.exception("Exception during background job.")
                pass
            finally:
                self.__background_job_queue.task_done()

    async def __test_connection(self):
        while "loop not canceled":
            await asyncio.sleep(5)  # TODO: Make configurable
            self.__logger.debug(f"Pinging sensor {self.__sensor}.")
            try:
                await asyncio.gather(*[self.__test_callback(sid) for sid in self.__callback_config.keys()])
            except asyncio.TimeoutError:
                asyncio.create_task(self.__shutdown())
                break

    async def __shutdown(self):
        try:
            [task.cancel() for task in self.__running_tasks if task is not asyncio.current_task()]
            try:
                await asyncio.gather(*self.__running_tasks)
            except asyncio.CancelledError:
                # We cancelled the tasks, so asyncio.CancelledError is expected.
                pass
        except Exception:
            self.__logger.exception("Error during shutdown of the sensor manager.")
        finally:
            self.__running_tasks.clear()

    async def disconnect(self):
        await self.__shutdown()

    async def run(self):
        self.__background_job_queue = asyncio.Queue()
        self.__running_tasks.append(asyncio.create_task(self.__background_job_manager()))
        self.__running_tasks.append(asyncio.create_task(self.__test_connection()))

        async for config in self.__parent.get_sensor_config(self.uid):
            await self.__init_sensor(config)

        self.__logger.info("Sensor '%s' connected.", self)


class Sensor(metaclass=ABCMeta):
    """
    Base class for all Tinkerforge API wrappers
    """

    @property
    def pid(self):
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
        if (last_update is None or last_update >= self.callback_period * 0.9):
            self.last_update = time.time()
            self.__callback_method(self, float(value), last_update)
        else:
            self.check_callback()
            self.logger.warning('Warning. Discarding value "%s" from sensor "%s", because the last update was too recent. Was %i s ago, but supposed to be every %i s.', value, self.uid, last_update / 1000, self.callback_period / 1000)

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
