# -*- coding: utf-8 -*-
"""
This file contains the wrapper for the Tinkerforge sensor class
"""
import asyncio
import logging
import time

from aiostream import pipe, stream
from tinkerforge_async.devices import GetCallbackConfiguration, ThresholdOption
from tinkerforge_async.device_factory import device_factory

from data_types import UpdateChangeEvent, RemoveChangeEvent

# Event bus topics
EVENT_BUS_BASE = "/sensors/tinkerforge"
EVENT_BUS_CONFIG_UPDATE_BY_UID = EVENT_BUS_BASE + "/by_uid/{uid}/update"
EVENT_BUS_DISCONNECT_BY_UID = EVENT_BUS_BASE + "/by_uid/{uid}/disconnect"
EVENT_BUS_STATUS = EVENT_BUS_BASE


class CallbackTestJob():
    """
    A job test a certain sensor callback on a device.
    """
    @property
    def sid(self):
        """
        Returns
        -------
        int
            The secondary id of the sensor
        """
        return self.__sid

    @property
    def timestamp(self):
        """
        Returns
        -------
        float
            The time in seconds since the epoch as a floating point number
        """
        return self.__timestamp

    @property
    def is_done(self):
        """
        Returns
        -------
        bool
            True if the job is done
        """
        return self.__is_done

    @is_done.setter
    def is_done(self, value):
        """
        Parameters
        ----------
        value: bool
            True if the job is done
        """
        self.__is_done = bool(value)

    def __init__(self, sid, timestamp):
        """
        Parameters
        ----------
        sid: int
            The secondary id of the sensor
        timestamp: float
            The time in seconds since the epoch as a floating point number
        """
        self.__sid = sid
        self.__timestamp = timestamp
        self.__is_done = False


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

    def __init__(self, device_id, uid, ipcon, event_bus, parent):
        self.__sensor = device_factory.get(device_id, uid, ipcon)
        self.__uuid = None
        self.__event_bus = event_bus
        self.__parent = parent
        self.__shutdown_event = asyncio.Event()
        self.__shutdown_event.set()   # Force the use of __aenter__()
        self.__logger = logging.getLogger(__name__)

    def __str__(self):
        return f"Tinkerforge {str(self.__sensor)}"

    def __repr__(self):
        return f"{self.__class__.__module__}.{self.__class__.__qualname__}(device_id={self.__sensor.DEVICE_IDENTIFIER!r}, uid={self.__sensor.uid}, ipcon={self.__sensor.ipcon!r})"

    async def __aenter__(self):
        self.__shutdown_event.clear()
        self.__event_bus.register(EVENT_BUS_DISCONNECT_BY_UID.format(uid=self.uid), self.__disconnect)
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.__event_bus.unregister(EVENT_BUS_DISCONNECT_BY_UID.format(uid=self.uid))
        self.__shutdown_event.set()

    async def __disconnect(self):
        self.__shutdown_event.set()

    async def __configure(self, config):
        self.__uuid = config.get('id')
        configured_callbacks = {}
        enabled_sids = set()
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
                    minimum=None,
                    maximum=None
                )
                try:
                    await self.__sensor.set_callback_configuration(sid, *callback_config)
                except AssertionError:
                    self.__logger.error("Invalid configuration for %s: sid=%i, config=%s", self.__sensor, sid, callback_config)
                else:
                    # Read back the callback config, because the sensor might have substituted values
                    callback_config = await self.__sensor.get_callback_configuration(sid)
                    configured_callbacks[sid] = [
                        last_update,
                        callback_config,
                    ]
                    if callback_config.period > 0:
                        enabled_sids.add(sid)
        return enabled_sids, configured_callbacks

    def __test_callback(self, callback_configs):
        async def tester(job):
            _, local_config = callback_configs[job.sid]

            remote_config = await self.__sensor.get_callback_configuration(job.sid)
            if remote_config != local_config:
                self.__logger.warning("Remote callback configuration %s (sid %i) was changed by a third party. Remote config: %s, Local config: %s", self.__sensor, job.sid, remote_config, local_config)
                await self.__sensor.set_callback_configuration(job.sid, *local_config)
            job.is_done = True
            return job
        return tester

    @staticmethod
    def __filter_running_jobs():
        """
        Filter out all jobs for sids, that are already being tested. Also
        filter out jobs, that are done.
        """
        running_jobs = set()    # A set of sids, which are being checked

        def filter_job(job):
            if job.is_done:
                running_jobs.remove(job.sid)
                return False
            if job.sid in running_jobs:
                return False
            running_jobs.add(job.sid)
            return True

        return filter_job

    async def callback_test_worker(self, configured_callbacks, ping_interval, input_queue):
        """
        Pulls new callback test jobs from the queue and also injects new jobs
        into the pipeline at `ping_interval`.
        """
        jobs = (
            stream.merge(
                stream.call(input_queue.get)
                | pipe.cycle(),  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
                stream.repeat(stream.iterate(configured_callbacks), interval=ping_interval)
                | pipe.flatten()    # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
                | pipe.map(lambda sid: CallbackTestJob(sid, 0))  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            )
            | pipe.filter(self.__filter_running_jobs())  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            | pipe.map(self.__test_callback(configured_callbacks))  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            | pipe.map(input_queue.put_nowait)  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
        )

        await jobs

    @staticmethod
    def is_no_event(event_type):
        """
        Filter out events by its tpye.
        """
        def filter_event(item):
            return not isinstance(item, event_type)

        return filter_event

    async def __configure_and_read(self, config, ping_interval, old_config=None):
        sids, configured_callbacks = await self.__configure(config)
        test_job_queue = asyncio.Queue()

        if sids:
            data_stream = (
                stream.merge(
                    stream.iterate(self.__sensor.read_events(sids=sids)),
                    stream.call(self.callback_test_worker, configured_callbacks, ping_interval, test_job_queue),
                    stream.iterate(self.__event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE_BY_UID.format(uid=self.uid)))
                )
                | pipe.takewhile(self.is_no_event(UpdateChangeEvent))  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            )

            async with data_stream.stream() as streamer:
                async for item in streamer:
                    # Test the callback in the background
                    sid, timestamp = item['sid'], item['timestamp']
                    last_update, local_config = configured_callbacks[sid]
                    if timestamp - last_update <= 0.9 * local_config.period / 1000:
                        test_job_queue.put_nowait(CallbackTestJob(sid, timestamp))
                    configured_callbacks[sid][0] = timestamp
                    # In the meantime, return the value
                    yield {
                        'timestamp': timestamp,
                        'sender': self,
                        'sid': sid,
                        'payload': item['payload'],
                        'topic': config['config'][str(item['sid'])]['topic']
                    }

    async def read_events(self, ping_interval):
        """
        Read the sensor data, and ping the sensor.

        Returns
        -------
        Iterator[dict]
            The sensor data.
        """
        assert ping_interval > 0
        config = await self.__event_bus.call("/database/tinkerforge/get_sensor_config", self.uid)

        new_streams_queue = asyncio.Queue()
        new_streams_queue.put_nowait(self.__configure_and_read(config, ping_interval))
        new_streams_queue.put_nowait(self.__event_bus.subscribe(EVENT_BUS_CONFIG_UPDATE_BY_UID.format(uid=self.uid)))
        new_streams_queue.put_nowait(stream.just(self.__shutdown_event.wait()))

        def filter_updates(item):
            if isinstance(item, UpdateChangeEvent):
                # Reconfigure the device, then start reading from it.
                # The __configure_and_read method, will automatically shut
                # down.
                nonlocal config, new_streams_queue
                old_config = config
                config = item.change
                new_streams_queue.put_nowait(self.__configure_and_read(config, ping_interval, old_config))
                return False
            return True

        merged_stream = (
            stream.call(new_streams_queue.get)
            | pipe.cycle()  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            | pipe.flatten()  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
            | pipe.filter(filter_updates)  # https://github.com/PyCQA/pylint/issues/3744 pylint: disable=no-member
        )
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                if self.__shutdown_event.is_set():
                    break
                yield item
