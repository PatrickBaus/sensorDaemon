"""
This is a wrapper for Tinkerforge devices.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from aiostream import async_, pipe, stream
from tinkerforge_async.devices import GetCallbackConfiguration, ThresholdOption
from tinkerforge_async.ip_connection import NotConnectedError

from async_event_bus import event_bus
from data_types import DataEvent
from helper_functions import call_safely, create_device_function
from errors import ConfigurationError


class TinkerforgeSensor:
    """This class extends the driver with catch-all arguments in the constructor"""
    @classmethod
    @property
    def driver(cls) -> str:
        """
        Returns
        -------
        str
            The driver that identifies it to the host factory
        """
        return "tinkerforge2"

    def __init__(
            self,
            device
    ) -> None:
        self._device = device
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def _stream_config_updates(sensor: TinkerforgeSensor) -> stream:
        """
        Tries to fetch a config from the database. It also listens to 'nodes/tinkerforge/$UID/update' for new configs
        from the database.
        Parameters
        ----------
        sensor: TinkerforgeSensor
            The brick or bricklet for which to fetch a config from the database
        Returns
        -------
        stream
        """
        return stream.chain(
            stream.call(call_safely, "db_tinkerforge_sensors/get_config", "db_tinkerforge_sensors/status_update", sensor._device.uid)
                | pipe.takewhile(lambda config: config is not None),
            stream.iterate(event_bus.subscribe(f"nodes/tinkerforge/{sensor._device.uid}/update"))
        )

    def stream_data(self):
        # Generates the first configuration
        # Query the database and if it does not have a config for the sensor, wait until there is one

        data_stream = (
            stream.chain(
                stream.just(self),
                stream.iterate(event_bus.subscribe(f"nodes/tinkerforge/{self._device.uid}/remove"))[:1] | pipe.map(lambda x: None)
            )
            | pipe.switchmap(
                lambda sensor: stream.empty() if sensor is None else (
                    self._stream_config_updates(sensor)
                    | pipe.switchmap(
                        lambda config: stream.chain(
                                stream.just(config),
                                stream.iterate(event_bus.subscribe(f"nodes/by_uuid/{config['uuid']}/remove"))[:1] | pipe.map(lambda x: None),
                            )
                    )
                    | pipe.action(
                        lambda config: logging.getLogger(__name__).info(
                            "Configuration update for Tinkerforge sensor: %s", sensor._device,
                        )
                    )
                    | pipe.map(self._create_config)
                    | pipe.switchmap(
                        lambda config: stream.empty() if config is None or not config['enabled'] else (
                            self._configure_and_stream(config)
                        )
                    )
                )
            )
        )

        return data_stream

    def _create_config(self, config):
        if config is None:
            return None
        try:
            on_connect = tuple(
                create_device_function(self._device, func_call) for func_call in config['on_connect']
            )
            config['on_connect'] = on_connect
        except ConfigurationError:
            config = None

        return config

    def _read_sensor(self, source_uuid: UUID, sid: int, unit: str, topic: str, callback_config: GetCallbackConfiguration):
        monitor_stream = (
            stream.repeat(self._device, interval=1)
            | pipe.map(async_(lambda sensor: sensor.get_callback_configuration(sid)))
            | pipe.map(lambda current_config: None if current_config == callback_config else self._device)
            | pipe.filter(lambda sensor: sensor is not None)
            | pipe.action(lambda sensor: logging.getLogger(__name__).info("Resetting callback config for %s", sensor))
            | pipe.action(async_(lambda sensor: sensor.set_callback_configuration(sid, *callback_config)))
            | pipe.filter(lambda x: False)
        )

        return stream.merge(
            stream.just(monitor_stream),
            stream.iterate(self._device.read_events(sids=(sid,)))
            | pipe.map(
                lambda item: DataEvent(
                    sender=source_uuid, topic=topic, value=item['payload'], sid=item['sid'], unit=unit
                )
            )
        )

    @staticmethod
    def _parse_callback_configuration(sid: str, config: dict[str, Any]):
        sid = int(sid)
        callback_config = GetCallbackConfiguration(
            period=config['interval'],
            value_has_to_change=config['trigger_only_on_change'],
            option=ThresholdOption.OFF,
            minimum=None,
            maximum=None
        )
        return sid, config['unit'], config['topic'], callback_config

    async def _set_callback_configuration(self, sid: int, unit: str, topic: str,  config: GetCallbackConfiguration):
        try:
            await self._device.set_callback_configuration(sid, *config)
        except AssertionError:
            self._logger.error("Invalid configuration for %s: sid=%i, config=%s", self._device, sid, config)
            return stream.empty()
        remote_callback_config: GetCallbackConfiguration
        remote_callback_config = await self._device.get_callback_configuration(sid)
        if remote_callback_config.period == 0:
            self._logger.warning(
                "Callback configuration configuration for %s: sid=%i, config=%s failed. Source disabled.",
                self._device,
                sid,
                config
            )
            return stream.empty()
        return stream.just((sid, unit, topic, remote_callback_config))

    def _configure_and_stream(self, config):
        if config is None:
            return stream.empty()
        try:
            config_stream = (
                stream.chain(
                    stream.iterate(config['on_connect'])
                    | pipe.concatmap(stream.call)
                    | pipe.concatmap(stream.just)
                    | pipe.filter(lambda result: False),
                    stream.iterate(config['config'].items())
                    | pipe.starmap(self._parse_callback_configuration)
                    | pipe.starmap(self._set_callback_configuration)
                    | pipe.flatten()
                    | pipe.map(lambda args: self._read_sensor(config['uuid'], *args))
                    | pipe.flatten()
                )
            )
            return config_stream
        except NotConnectedError:
            # Do not log it
            raise
        except Exception:
            self._logger.exception("This should not happen")
            raise
