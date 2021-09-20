# -*- coding: utf-8 -*-
"""
This file contains the wrapper for the Tinkerforge sensor class
"""
import asyncio
import logging
import time

from tinkerforge_async.devices import GetCallbackConfiguration, ThresholdOption
from tinkerforge_async.device_factory import device_factory
from tinkerforge_async.ip_connection import NotConnectedError


# Event bus topics
EVENT_BUS_BASE = "/sensors/tinkerforge"
EVENT_BUS_CONFIG_UPDATE_BY_UID = EVENT_BUS_BASE + "/by_uid/{uid}/update"
EVENT_BUS_DISCONNECT_BY_UID = EVENT_BUS_BASE + "/by_uid/{uid}/disconnect"
EVENT_BUS_STATUS = EVENT_BUS_BASE


class TinkerforgeSensor():
    """
    The wrapper class for all Tinkerforge sensors. It combines the database config
    with the underlying hardware.
    """
    @property
    def uuid(self):
        """
        Returns
        -------
        uuid.UUID
            The sensor config uuid.
        """
        return self.__uuid

    @property
    def uid(self):
        """
        Returns
        -------
        int
            The sensor hardware uid.
        """
        return self.__sensor.uid

    @property
    def host(self):
        """
        Returns
        -------
        TinkerforgeSensorHost
            The Tinkerforge host the sensor is connected to.
        """
        return self.__parent

    def __init__(self, device_id, uid, ipcon, parent):
        self.__sensor = device_factory.get(device_id, uid, ipcon)
        self.__uuid = None
        self.__parent = parent
        self.__logger = logging.getLogger(__name__)
        self.__callback_config = {}

    def __str__(self):
        return f"Tinkerforge {str(self.__sensor)}"

    def __repr__(self):
        return f"{self.__class__.__module__}.{self.__class__.__qualname__}(device_id={self.__sensor.DEVICE_IDENTIFIER!r}, uid={self.__sensor.uid}, ipcon={self.__sensor.ipcon!r})"

    async def __configure(self, config):
        self.__uuid = config.get('id')
        self.__callback_config = {}
        configured_sids = set()
        if self.__uuid is not None:
            on_connect = config.get("on_connect")
            if on_connect is not None:
                for cmd in on_connect:
                    try:
                        function = getattr(self.__sensor, cmd["function"])
                    except AttributeError:
                        self.__logger.error("Invalid configuration parameter '%s' for sensor %s", cmd["function"], self.__sensor)
                        continue

                    try:
                        result = function(*cmd.get("args", []), **cmd.get("kwargs", {}))
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:   # pylint: disable=broad-except
                        # Catch all exceptions and log them, because this is an external input
                        self.__logger.exception("Error processing config for sensor %s on host '%s'", self.__sensor.uid, self.__parent.hostname)
                        continue
            # configure the callbacks
            for sid, sid_config in config['config'].items():
                sid = int(sid)
                last_update = time.time() - sid_config['interval'] / 1000
                callback_config = GetCallbackConfiguration(
                    period=sid_config['interval'],
                    value_has_to_change=sid_config['trigger_only_on_change'],
                    option=ThresholdOption.OFF,
                    minimum=0,
                    maximum=0
                )
                try:
                    await self.__sensor.set_callback_configuration(sid, *callback_config)
                except AssertionError:
                    self.__logger.error("Invalid configuration for %s: sid=%i, config=%s", self.__sensor, sid, callback_config)
                else:
                    self.__callback_config[sid] = (
                        last_update,
                        callback_config,
                    )
                if callback_config.period > 0:
                    configured_sids.add(sid)
        return configured_sids

    async def __test_callback(self, sid):
        remote_config = await self.__sensor.get_callback_configuration(sid)
        _, local_config = self.__callback_config[sid]
        if remote_config != local_config:
            self.__logger.warning("Remote callback configuration was changed by a third party. Remote config: {%s}, Local config: {%s}", remote_config, local_config)
            await self.__sensor.set_callback_configuration(sid, *local_config)
        return sid

    async def __data_producer(self, sids, output_queue):
        async for data in self.__sensor.read_events(sids=sids):
            output_queue.put_nowait(data)

    async def __disconnect_watcher(self, event_bus):
        async for _ in event_bus.subscribe(EVENT_BUS_DISCONNECT_BY_UID.format(uid=self.uid)):
            break

    async def __config_update_producer(self, event_bus, output_queue):
        async for data in event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE_BY_UID.format(uid=self.uid)):
            output_queue.put_nowait(data)

    async def __read_data(self, event_bus, output_queue):
        pending = set()
        critical_tasks = set()
        config_queue = asyncio.Queue()
        config_update_producer_task = asyncio.create_task(
            self.__config_update_producer(
                event_bus,
                output_queue=config_queue
            ),
            name=f"TF sensor {self.__sensor.uid} config producer."
        )
        pending.add(config_update_producer_task)
        critical_tasks.add(config_update_producer_task)
        config_update_task = asyncio.create_task(config_queue.get())
        pending.add(config_update_task)

        # Initial config
        config = await event_bus.call("/database/tinkerforge", self.uid)
        if config is not None:
            sids = await self.__configure(config)
            if sids:
                data_producer_task = asyncio.create_task(self.__data_producer(sids, output_queue), name=f"TF sensor {self.__sensor.uid} data producer.")
                pending.add(data_producer_task)
        else:
            data_producer_task = None

        try:
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task == config_update_task:
                        try:
                            config = task.result()
                            pending.discard(data_producer_task)
                            if data_producer_task is not None:
                                data_producer_task.cancel()     # No need to await that task
                            sids = await self.__configure(config)
                            config_update_task = asyncio.create_task(config_queue.get())
                            pending.add(config_update_task)
                            if sids:
                                data_producer_task = asyncio.create_task(self.__data_producer(sids, output_queue), name=f"TF sensor {self.__sensor.uid} data producer.")
                        finally:
                            config_queue.task_done()
                    elif task in critical_tasks:
                        task.result()
                        return
        finally:
            for task in pending:
                if not task.done():
                    task.cancel()
            results = await asyncio.gather(*pending, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    raise result

    async def read_events(self, event_bus, ping_interval):
        """
        Read the sensor data, and ping the sensor.

        Returns
        -------
        Iterator[dict]
            The sensor data.
        """
        pending = set()
        critical_tasks = set()  # Add task, that should never terminate to this set. The sensor will shut down if one of them returns.

        is_disconnected_task = asyncio.create_task(self.__disconnect_watcher(event_bus))
        pending.add(is_disconnected_task)
        critical_tasks.add(is_disconnected_task)

        ping_sensor_task = asyncio.create_task(self.__test_connection(ping_interval), name=f"Ping_task_{self.__sensor.uid}")
        pending.add(ping_sensor_task)
        critical_tasks.add(ping_sensor_task)

        data_queue = asyncio.Queue()
        data_producer_task = asyncio.create_task(self.__read_data(event_bus, data_queue), name=f"TF sensor {self.__sensor.uid} data producer.")
        pending.add(data_producer_task)
        critical_tasks.add(data_producer_task)
        data_task = asyncio.create_task(data_queue.get())
        pending.add(data_task)

        check_callback_jobs = {}    # {uid : task}

        # Finally publish, that we are ready
        event_bus.publish(EVENT_BUS_STATUS, self.uid)
        try:
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task in critical_tasks:
                        # This job was supposed to live happliy ever after, so the fairy tale is over now
                        task.result()   # Open the booby trap
                        return
                    if task in check_callback_jobs.values():
                        sid = task.result()
                        del check_callback_jobs[sid]
                    else:
                        event = task.result()
                        sid = event['sid']
                        # We ignore the value, if we haven't registered for that secondary id
                        if sid in self.__callback_config:
                            last_update, config = self.__callback_config[sid]
                            if event['timestamp'] - last_update <= 0.9 * config.period / 1000:
                                self.__logger.debug("Callback %s was %d} ms too soon. Callback is set to %i ms.", event, config.period / 1000 - (event['timestamp'] - last_update), config.period)
                                # If the event was too early check the callback. Maybe it was
                                # modified by someone else. We do not drop the event.
                                if sid not in check_callback_jobs:
                                    # Schedule a test of the callback config with a background job
                                    # worker. It will take care of cleanup as well.
                                    # Skip the test if we have a test scheduled already
                                    check_callback_jobs[sid] = asyncio.create_task(self.__test_callback(sid))
                                    pending.add(check_callback_jobs[sid])
                            self.__callback_config[sid] = (event['timestamp'], config)      # update the last_update field
                            data_task = asyncio.create_task(data_queue.get())
                            pending.add(data_task)
                            yield event     # Yield the event, no matter if it was too early
        finally:
            for task in pending:
                if not task.done():
                    task.cancel()
            results = await asyncio.gather(*pending, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    raise result

    async def __test_connection(self, interval):
        while "loop not cancelled":
            await asyncio.sleep(interval)
            self.__logger.debug("Pinging sensor %s.", self.__sensor)
            try:
                await asyncio.gather(*[self.__test_callback(sid) for sid in self.__callback_config])
            except (asyncio.TimeoutError, NotConnectedError):
                break
