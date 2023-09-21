#!/usr/bin/env python3
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
from __future__ import annotations

import asyncio
import logging
import signal

# noinspection PyPackageRequirements
import uuid
from uuid import UUID

from decouple import UndefinedValueError, config

from _version import __version__
from managers import DatabaseManager, HostManager, MqttManager


def load_secret(name: str, *args, **kwargs) -> str:
    """
    Loads a sensitive parameter either from the environment variable or Docker secret file. See
    https://docs.docker.com/engine/swarm/secrets/ for details. The env variable read for the file
    path is automatically appended by '_FILE'. Using the name 'MY_SECRET', the secret is either read from 'MY_SECRET' or
    the secret path is read from 'MY_SECRET_FILE'. In the latter case the secret is then read from  the file
    'MY_SECRET_FILE'. The secret file is preferred over the env variable.

    Parameters
    ----------
    name: str
        The name of the env variable containing the secret or the file path.
    Returns
    -------
    str:
        The secret read either from the environment or secrets file.
    """
    # Remove the default return value for testing against the secrets file, we will put it back later.
    if "default" in kwargs:
        has_default = True
        default_value = kwargs.pop("default")
    else:
        has_default = False
        default_value = None
    try:
        with open(
            config(f"{name}_FILE", *args, **kwargs), newline=None, mode="r", encoding="utf-8"
        ) as secret_file:  # pylint: disable=unspecified-encoding
            return secret_file.read().rstrip("\n")
    except UndefinedValueError:
        pass

    if has_default:
        kwargs["default"] = default_value
    try:
        return config(name, *args, **kwargs)
    except UndefinedValueError:
        raise UndefinedValueError(f"{name} not found. Declare it as envvar or define a default value.") from None


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

    async def run(self):  # pylint: disable=too-many-locals
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
            asyncio.get_running_loop().add_signal_handler(sig, lambda: asyncio.create_task(self.__shutdown()))

        # Read either environment variable, settings.ini or .env file
        database_params = {"host": config("SENSORS_DATABASE_HOST")}
        database_params["username"] = load_secret("SENSORS_DATABASE_USERNAME", default=None)
        database_params["password"] = load_secret("SENSORS_DATABASE_PASSWORD", default=None)
        # wamp_host = config('WAMP_HOST')
        # wamp_port = config('WAMP_PORT', cast=int, default=18080)
        # wamp_url = f"ws://{wamp_host}:{wamp_port}/ws"
        # realm = "com.leapsight.test"
        mqtt_host = config("MQTT_HOST", default="localhost")
        mqtt_port = config("MQTT_PORT", cast=int, default=1883)
        try:
            node_id = config("NODE_ID", cast=UUID)
        except UndefinedValueError:
            node_id = None

        if node_id is None:
            self.__logger.warning(
                "No node is set. How about setting 'NODE_ID=%s'? I won't use a node id for now.", uuid.uuid4()
            )
        else:
            self.__logger.warning("This is the node with id: %s", node_id)

        mqtt_manager = MqttManager(node_id=node_id, host=mqtt_host, port=mqtt_port)
        database_manager = DatabaseManager(**database_params)
        host_manager = HostManager(node_id=node_id)

        tasks = set()
        mqtt_task = asyncio.create_task(mqtt_manager.run())
        tasks.add(mqtt_task)
        shutdown_event_task = asyncio.create_task(self.__shutdown_event.wait())
        tasks.add(shutdown_event_task)
        database_task = asyncio.create_task(database_manager.run())
        tasks.add(database_task)
        host_manager_task = asyncio.create_task(host_manager.run())
        tasks.add(host_manager_task)

        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is shutdown_event_task:
                    self.__logger.warning("Received shutdown request. Stopping workers.")
                    for pending_task in pending:
                        pending_task.cancel()
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
        # In case the shutdown hangs, we will kill it now.
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
        # Silently swallow the error to suppress the noise, then terminate.
        pass


def parse_log_level(log_level: int | str) -> int:
    """
    Parse an int or string, then return its standard log level definition.
    Parameters
    ----------
    log_level: int or str
        The log level. Either a string or a number.
    Returns
    -------
    int
        The log level as defined by the standard library. Returns logging.INFO as default
    """
    try:
        level = int(log_level)
    except ValueError:
        # parse the string
        level = logging.getLevelName(str(log_level).upper())
    if isinstance(level, int):
        return level
    return logging.INFO  # default log level


# Report all mistakes managing asynchronous resources.
# import warnings
# warnings.simplefilter('always', ResourceWarning)
logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
    level=config("APPLICATION_LOG_LEVEL", default=logging.INFO, cast=parse_log_level),
    datefmt="%Y-%m-%d %H:%M:%S",
)

asyncio.run(main())
