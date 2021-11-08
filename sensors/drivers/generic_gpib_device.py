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
This is a asyncIO driver for the HP 3478A DMM to abstract away the GPIB interface.
"""

import asyncio
from decimal import Decimal
from enum import Enum, Flag
from math import log
import re   # Used to test for numerical return values


class GenericGpibDevice:
    """
    The driver for the HP 3478A 5.5 digit multimeter. It support both linux-gpib and the Prologix
    GPIB adapters.
    """
    @property
    def connection(self):
        """
        Returns
        ----------
        AsyncGpib or AsyncPrologixGpibController
            The GPIB connection
        """
        return self.__conn

    def __init__(self, connection):
        self.__conn = connection

    def __str__(self):
        return f"Generic GPIB device at {str(self.__conn)}"

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.disconnect()

    async def get_id(self):
        """
        The HP 3478A does not support an ID request, so we will report a constant for compatibility
        reasons. The method is not async, but again for compatibility reasons with other drivers,
        it is declared async.
        """
        return "Generic GPIB device"

    async def connect(self):
        """
        Connect the GPIB connection and configure the GPIB device for the DMM.
        """
        await self.__conn.connect()
        if hasattr(self.__conn, "set_eot"):
            # Used by the Prologix adapters
            await self.__conn.set_eot(False)

    async def disconnect(self):
        """
        Disconnect the GPIB device and release any lock on the front panel of the device if held.
        """
        try:
            await self.__conn.ibloc()
        except ConnectionError:
            pass
        finally:
            await self.__conn.disconnect()

    async def read(self, length=None):
        """
        Read a single value from the device. If `length' is given, read `length` bytes, else
        read until a line break.

        Parameters
        ----------
        length: int, default=None
            The number of bytes to read. Ommit to read a line.

        Returns
        -------
        Decimal or bytes
            Either a value or a number of bytes as defined by `length`.
        """
        if length is None:
            result = (await self.__conn.read()).rstrip(b'\r\n')    # strip the EOT characters (\r\n)
        else:
            result = await self.__conn.read(length=length)

        return result   # else return the bytes

    async def read_all(self, length=None):
        """
        Read a all values from the device. If `length' is given, read `length` bytes, else
        read until a line break, then yield the result.

        Parameters
        ----------
        length: int, default=None
            The number of bytes to read. Ommit to read a line.

        Returns
        -------
        Iterator[Decimal or bytes]
            Either a value or a number of bytes as defined by `length`.
        """
        while 'loop not cancelled':
            try:
                await self.connection.wait((1 << 11) | (1<<14))
                result = await self.read(length)
                yield result
            except asyncio.TimeoutError:
                pass

    async def query(self, command, length=None):
        await self.write(command)
        result = await self.read(length=length)
        return result.decode('ascii')

    async def write(self, msg):
        """
        Write data or commands to the instrument. Do not terminated the command with a new line or
        carriage return (\r\n).

        Parameters
        ----------
        msg: str or bytes
            The string to be sent to the device.
        """
        msg = msg.encode('ascii')
        await self.__conn.write(msg)

    async def serial_poll(self):
        """
        Serial poll the device/GPIB controller.
        """
        return SerialPollFlags(await self.__conn.serial_poll())
