#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ##### BEGIN GPL LICENSE BLOCK #####
#
# Copyright (C) 2021 Patrick Baus
# This file is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This file is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this file.  If not, see <http://www.gnu.org/licenses/>.
#
# ##### END GPL LICENSE BLOCK #####
"""
This is a asyncIO driver for a generic SCPI compatible device.
"""

import asyncio
import logging
from decimal import Decimal, InvalidOperation

from aiostream import stream, pipe

from data_types import DataEvent, RemoveChangeEvent, AddChangeEvent, UpdateChangeEvent


class IncompatibleDeviceException(Exception):
    pass


class GenericDevice:
    SEPARATOR = "\n"  # The default message separator (will be encoded to bytes later)

    @property
    def uuid(self):
        """
        Returns
        ----------
        Uuid The uuid of the sensor
        """
        return self.__uuid

    @property
    def connection(self):
        """
        Returns
        ----------
        AsyncGpib or AsyncPrologixGpibController
            The GPIB connection
        """
        return self.__conn

    def __init__(self, uuid, connection, event_bus):
        self.__uuid = uuid
        self.__config = dict()
        self.__device_name = None
        self.__conn = connection
        self.__event_bus = event_bus
        self.__logger = logging.getLogger(__name__)
        self.__is_shut_down = True
        self.__is_restarting = False

    def __str__(self):
        return f"{self.get_name()} at {str(self.__conn)}"

    async def __aenter__(self):
        self.__is_restarting = False
        # Wait for the device to finish any commands still pending
        # This is required to make sure we are synced
        await self.wait_for_opc(self.connection.connection_timeout)

        retries_left = 3
        while "device has no id":
            try:
                device_id = await self.get_id()
                break
            except ValueError:
                if retries_left > 0:
                    retries_left -= 1
                else:
                    raise IncompatibleDeviceException("Device does not support the *IDN? command") from None
        self.__device_name = f"{device_id[0]} {device_id[1]} ({device_id[2]})"
        self.__is_shut_down = False
        self.__logger.info("Connected to %s", self)
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        try:
            # Reset the device to local mode
            await self.write("SYSTem:LOCal")
            self.__is_shut_down = True
            self.__logger.info("Disconnected from %s", self)
            if self.__is_restarting:
                # Send our config to the host, to create a new sensor
                self.__event_bus.publish(
                    f"devices/by_uid/{self.__config['host']}/update",
                    AddChangeEvent(self.__config)
                )
                self.__is_restarting = False
            # Do not disconnect the connection. This is up to the host
        except Exception:
            # If this fails, there is not much we can do
            pass

    async def wait_for_opc(self, timeout=None):
        await self.write("*OPC?")
        while (await asyncio.wait_for(self.read(), timeout=timeout)) != '1':
            await asyncio.sleep(0.1)

    async def get_id(self):
        """
        Returns a list with 4 elements, that contain the manufacturer name, model number, serial number and revision
        :return:
        """
        result = await self.query("*IDN?")
        result = result.split(",")
        if len(result) != 4:
            raise ValueError("Device returned invalid ID: %r", result)
        return result

    def get_name(self):
        return "Generic SCPI device" if self.__device_name is None else self.__device_name

    async def read(self, length=None, separator=None):
        """
        Read a single value from the device. If `length' is given, read `length` bytes, else
        read until a line break.

        Parameters
        ----------
        length: int, default=None
            The number of bytes to read. Omit to read a line.

        separator: str, default=None
            One or more characters, that separate replies. If not set, the default
            separator '\n' will be used if length is not set.
        -------
        Decimal or bytes
            Either a value or a number of bytes as defined by `length`.
        """
        if length is None:
            # use the default separator if none is given
            try:
                separator = self.SEPARATOR.encode() if separator is None else separator.encode()
            except UnicodeEncodeError:
                self.__logger.warning("Invalid separator '%r', using default: '%r'", separator, self.SEPARATOR)
                separator = self.SEPARATOR.encode()

        data = await self.connection.read(length, separator)
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            # drop it. It is not SCPI compliant
            self.__logger.error("Invalid data read '%r'. This driver requires ASCII or UTF-8 data", data)

    async def write(self, cmd, separator=None):
        if separator is None:
            cmd += self.SEPARATOR
        else:
            cmd += separator
        try:
            await self.connection.write(cmd.encode())
        except UnicodeEncodeError:
            self.__logger.error("Cannot write invalid command '%r'. Use ASCII or UTF-8 strings only.", cmd)

    async def query(self, cmd, length=None, separator=None):
        await self.write(cmd, separator)
        result = await self.read(length, separator)
        return result

    async def __run_command(self, cmd, timeout, *args, **kwargs):
        function = getattr(self, cmd)  # Raises AttributeError if not found
        # Run the command and if is a coroutine, await it
        result = function(*args, **kwargs)    # May raise any error
        if asyncio.iscoroutine(result):
            result = await asyncio.wait_for(result, timeout=timeout)
        return result

    async def configure(self, config):
        """
        Runs a number of function calls on the device. To make sure the device is done, run `wait_for_opc()` as the
        last command.
        :param config:
        :return:
        """
        self.__config = config
        for cmd in config.get('on_connect', []):
            try:
                await self.__run_command(
                    cmd['function'],
                    None if cmd.get('timeout') is None else cmd['timeout']/1000,
                    *cmd.get('args', []),
                    **cmd.get('kwargs', {})
                )
            except AttributeError:
                self.__logger.error("Invalid configuration parameter '%s' for device ", cmd['function'],
                                    self)
            except Exception:   # pylint: disable=broad-except
                # Catch all exceptions and log them, because this is an external input
                self.__logger.exception("Error processing config for device %s", self)

    async def __data_producer(self):
        cmd, interval = self.__config['on_read'], self.__config['interval']/1000
        retry_counter = 0   # Counts the number of retries in case of a timeout
        while "sensor connected":
            start = asyncio.get_running_loop().time()
            try:
                result = await self.__run_command(
                    cmd['function'],
                    None if cmd.get('timeout') is None else cmd['timeout']/1000,
                    *cmd.get('args', []),
                    **cmd.get('kwargs', {})
                )
                result = Decimal(result)
                retry_counter = 0
                yield DataEvent(
                    sender=self,
                    sid=0,
                    driver=self.__config['driver'],
                    topic=self.__config['topic'],
                    value=result
                )
            except AttributeError:
                # Raised by __run_command
                self.__logger.error("Invalid read command '%s' for sensor on %s", cmd['function'],
                                    self.connection)
                break
            except InvalidOperation:
                # Raised by Decimal, if the result is not a number
                self.__logger.error("Sensor %s did not return a valid number. Dropping result: %r", self, result)
            except asyncio.TimeoutError:
                # We will ignore 3 timeouts, then it will be escalated to the host, possibly for reconnection
                retry_counter += 1
                if retry_counter <= 3:
                    self.__logger.warning("Timeout while reading %s. Check the connection or settings", self)
                else:
                    raise
            except Exception:  # pylint: disable=broad-except
                # Catch all exceptions and log them, because this is an external input
                self.__logger.exception("Error processing read command for sensor on %s", self.connection)
                break
            # If we have taken less than $interval time, then we will sleep for the remaining time
            await asyncio.sleep(interval-asyncio.get_running_loop().time()+start)

    async def __update_listener(self):
        """
        If we receive an update that changes either the host driver or the remote
        connection, we will tear down this host and replace it with a new one.
        """
        event_topic = f"devices/by_uid/{self.uuid}/update"
        self.__logger.debug("%s listening to %s", self, event_topic)
        async for event in self.__event_bus.subscribe(event_topic):
            if isinstance(event, UpdateChangeEvent):
                self.__config = event.change
            yield event

    async def read_device(self):
        """
        Returns all data from all configured sensors connected to the host.

        Returns
        -------
        Iterator[DataEvent]
            The sensor data.
        """
        if self.__is_shut_down:
            # Error out if the user is not using the proper context manager
            raise TypeError("This method can only be run inside its context. Use a contextmanager.")
        new_streams_queue = asyncio.Queue()  # Add generators here to add them to the output
        new_streams_queue.put_nowait(stream.iterate(self.__data_producer()))  # Generates new sensors for the initial config and if the config changes
        new_streams_queue.put_nowait(stream.iterate(self.__update_listener()))  # Sets the shutdown event if the driver, hostname or port of this config is changed

        # For details on using aiostream, check here:
        # https://aiostream.readthedocs.io/en/stable/core.html#stream-base-class
        merged_stream = stream.call(new_streams_queue.get) | pipe.cycle() | pipe.flatten()   # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                if isinstance(item, DataEvent):
                    yield item
                elif isinstance(item, RemoveChangeEvent):
                    break
                elif isinstance(item, UpdateChangeEvent):
                    # Restart the device with the new configuration
                    self.__is_restarting = True
                    break
