#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ##### BEGIN GPL LICENSE BLOCK #####
#
# Copyright (C) 2022  Patrick Baus
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
"""
Kraken is a sensor data aggregation tool for distributed sensors arrays. It uses
AsyncIO instead of threads to scale and outputs data to a MQTT broker.
"""

import asyncio
import logging

from autobahn.asyncio.component import Component, run
from autobahn.wamp import SessionNotReady


class WampWrapper:
    def __init__(self, transports, realm, timeout=2.5):
        """
        Creates a sensorDaemon object.
        """
        self.__logger = logging.getLogger(__name__)
        self.__transports = transports
        self.__pending = set()
        self.__realm = realm
        self.__session = None
        self.__timeout = timeout

    async def run_wamp(self, component):
        await run([component], start_loop=False, log_level=None)

    async def __aenter__(self):
        component = Component(
            transports=self.__transports,
            realm=self.__realm,
        )

        fut = asyncio.get_running_loop().create_future()
        wamp_task = None    # Will be set later, but is needed by the callbacks

        @component.on_join
        async def join(session, details):
            self.__session = session
            fut.set_result((session, details))

        @component.on_leave
        def leave(session, details):
            self.__pending.discard(wamp_task)

        @component.on_connectfailure
        async def connection_failure(session, reason):
            fut.set_exception(ConnectionError(reason))

        wamp_task = asyncio.create_task(self.run_wamp(component))
        try:
            self.__pending.add(wamp_task)
            result = await fut
        except ConnectionError as e:
            component.stop()
            try:
                await asyncio.wait_for(wamp_task, timeout=.1)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(e)
            finally:
                # Remove the task from the list of running tasks on error
                self.__pending.discard(wamp_task)
            raise e
        return result

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        if self.__session:
            try:
                await self.__session.leave()
            except SessionNotReady:
                # Drop this, as we cannot shut down a session, that is not established
                pass
            except Exception:
                self.__logger.exception("Error during shutdown of the WAMP session")

            # By the time we reach this line, self.__pending should be empty,
            # if it is not empty, reap the tasks, that are hanging.
            [task.cancel() for task in self.__pending]
            try:
                await asyncio.gather(*self.__pending)
            except asyncio.CancelledError:
                pass
