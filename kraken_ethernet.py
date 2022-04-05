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
import signal
from contextlib import AsyncExitStack

from aiostream import stream, pipe
from autobahn.asyncio.component import Component, run
from autobahn.wamp.types import RegisterOptions
# noinspection PyPackageRequirements
from decouple import config

from _version import __version__
from async_event_bus import AsyncEventBus
from data_types import ChangeType
from database.models import SensorHost

from databases import MongoDb
from managers import WampManager, DatabaseManager, EthernetSensorManager, MqttManager
from sensors.generic_ethernet_host import EthernetSensorHost
from wamp_wrapper import WampWrapper


class Kraken:
    """
    Main daemon, that runs in the background and monitors all sensors. It will
    configure them according to options set in the database and then place the
    returned data in the database as well.
    """

    def __init__(self):
        """
        Creates a sensorDaemon object.
        """
        self.__logger = logging.getLogger(__name__)
        self.__shutdown_event = asyncio.Event()

    @staticmethod
    async def host_producer(driver):
        async for host in SensorHost.find(SensorHost.driver == driver):
            yield (ChangeType.ADD, host)

        # Now monitor the database for changes
        resume_token = None
        aggregation_pipeline = [
            # Stage 1: Filter for hosts with the correct driver
            {
                "$match": {"driver": driver}
            },
        ]
        async with SensorHost.get_motor_collection().watch(
                [], full_document="updateLookup", resume_after=resume_token) as change_stream:
            async for change in change_stream:
                # TODO: catch parser errors!
                if change['operationType'] == 'delete':
                    yield (ChangeType.REMOVE, change['documentKey']['_id'])
                elif change['operationType'] == "update" or change['operationType'] == "replace":
                    yield (ChangeType.UPDATE, SensorHost.parse_obj(change['fullDocument']))
                elif change['operationType'] == "insert":
                    yield (ChangeType.ADD, SensorHost.parse_obj(change['fullDocument']))

    async def data_producer(self, host):
        # connect to the Ethernet host, then start collecting data
        retry_count = 0
        while "Host has not shut down":
            try:
                async with host as connected_host:
                    async for data in connected_host.read_data():
                        yield data
                break   # If we get here, it means, the host has stopped yielding data (aka shut down)
            except asyncio.exceptions.TimeoutError:
                if retry_count < 3:
                    failure_str = f"({retry_count} times)" if retry_count > 1 else ""
                    self.__logger.warning("Failed to connect to host '%s:%i' %s. Error: Timeout. Retrying",
                                          failure_str,
                                          self.hostname,
                                          self.port
                    )
                elif retry_count == 3:
                    self.__logger.warning(
                        "Failed to connect to host '%s:%i' (%i times). Error: Timeout. Suppressing warnings from "
                        "hereon.",
                        retry_count,
                        host.hostname,
                        host.port
                    )
            except ConnectionError as exc:
                if retry_count < 3:
                    self.__logger.error("Failed to connect to host '%s:%i'.", host.hostname, host.port)
            except OSError as exc:
                if retry_count < 3:
                    self.__logger.error(
                        "Failed to connect to host '%s:%i'. Error %i.",
                        host.hostname,
                        host.port,
                        exc.errno
                    )
            except Exception:
                self.__logger.exception("Host %s raised an exception.", host.uuid)

            # TODO: Add better delay
            await asyncio.sleep(2)
            retry_count += 1

    async def run(self):
        """
        Start the daemon and keep it running through the while (True)
        loop. Execute shutdown() to kill it.
        """
        self.__logger.warning("#################################################")
        self.__logger.warning("Starting Kraken v%s...", __version__)
        self.__logger.warning("#################################################")

        # Catch signals and shutdown
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for sig in signals:
            asyncio.get_running_loop().add_signal_handler(
                sig, lambda: asyncio.create_task(self.__shutdown()))

        # Read either environment variable, settings.ini or .env file
        database_url = config('SENSORS_DATABASE_HOST')
        wamp_host = config('WAMP_HOST')
        wamp_port = config('WAMP_PORT', cast=int, default=18080)
        wamp_url = f"ws://{wamp_host}:{wamp_port}/ws"
        realm = "com.leapsight.test"
        mqtt_host = config('MQTT_HOST', default="localhost")
        mqtt_port = config('MQTT_PORT', cast=int, default=1883)

        # The event bus to loosely couple all three components:
        # The database, the sensors and the PubSub network
        event_bus = AsyncEventBus()

        #wamp_manager = WampManager(url=wamp_url, realm=realm, event_bus=event_bus)
        mqtt_manager = MqttManager(host=mqtt_host, port=mqtt_port, event_bus=event_bus)
        database_manager = DatabaseManager(url=database_url, event_bus=event_bus)
        sensor_manager = EthernetSensorManager(event_bus=event_bus)

        tasks = set()
        #wamp_task = asyncio.create_task(wamp_manager.run())
        #tasks.add(wamp_task)
        mqtt_task = asyncio.create_task(mqtt_manager.run())
        tasks.add(mqtt_task)
        shutdown_event_task = asyncio.create_task(self.__shutdown_event.wait())
        tasks.add(shutdown_event_task)
        database_task = asyncio.create_task(database_manager.run())
        tasks.add(database_task)
        sensor_task = asyncio.create_task(sensor_manager.run())
        tasks.add(sensor_task)

        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                if t is shutdown_event_task:
                    [task.cancel() for task in pending]
                    try:
                        await asyncio.gather(*pending)
                    except asyncio.CancelledError:
                        pass
                else:
                    # TODO: We have an error...do sth.
                    pass
        finally:
            await self.shutdown()

         # @component.register("com.kraken.database.tinkerforge.get", options=RegisterOptions(details_arg='details'))
        # async def get_sensor(uid, *args, **kwargs):
        #     sensor_config = dict(await TinkerforgeSensor.find_one(TinkerforgeSensor.uid ==   ))
        #     print(sensor_config)
        #     sensor_config['id'] = str(sensor_config['id'])
        #     return sensor_config

    async def __shutdown(self):
        self.__shutdown_event.set()

    async def shutdown(self):
        """
        Stops the daemon and gracefully disconnect from all clients.
        """
        self.__logger.warning("#################################################")
        self.__logger.warning("Stopping Kraken...")
        self.__logger.warning("#################################################")

        # We should never have to do anything here, because if the __shutdown_event is set, the main loop shuts down and
        # cleans up.
        # In case the shutdown hangs, we will kill it now
        # Get all running tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        # and stop them
        [task.cancel() for task in tasks]  # pylint: disable=expression-not-assigned
        # finally, wait for them to terminate
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        except Exception:  # pylint: disable=broad-except
            # We want to catch all exceptions on shutdown, except the asyncio.CancelledError
            # The exception will then be printed using the logger
            self.__logger.exception("Error while reaping tasks during shutdown")


async def main():
    """
    The main (infinite) loop, that runs until Kraken has shut down.
    """
    daemon = Kraken()
    try:
        await daemon.run()
    except asyncio.CancelledError:
        # Swallow that error, because this is the root task, there is nothing
        # to cancel above it.
        pass

# Report all mistakes managing asynchronous resources.
# import warnings
# warnings.simplefilter('always', ResourceWarning)
logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
    level=logging.INFO,  # Enable logs from the ip connection. Set to debug for even more info
    datefmt='%Y-%m-%d %H:%M:%S'
)

asyncio.run(main())
