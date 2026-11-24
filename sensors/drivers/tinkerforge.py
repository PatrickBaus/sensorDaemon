"""
This is a wrapper for Tinkerforge devices.
"""

from __future__ import annotations

# pylint: disable=duplicate-code
import asyncio
import logging
import struct
from functools import partial
from types import TracebackType
from typing import Any, AsyncGenerator, Self, TypeAlias
from uuid import UUID

from aiostream import Stream, async_, pipe, stream
from tinkerforge_async.devices import (
    AdvancedCallbackConfiguration,
    Device,
    InvalidSidError,
    SimpleCallbackConfiguration,
    ThresholdOption,
)

from async_event_bus import event_bus
from data_types import DataEvent
from database.models import TinkerforgeSensorConfigModel
from errors import ConfigurationError
from helper_functions import call_safely, context, create_device_function
from sensors.drivers.shared import DeviceStatus

# The errors a device function raises for an invalid configuration: a wrong number or type of arguments (TypeError), a
# value the device does not accept (ValueError, which includes InvalidSidError), a value that cannot be converted
# (ArithmeticError, e.g. decimal.InvalidOperation), or a value out of range of the Tinkerforge protocol (struct.error).
INVALID_CONFIGURATION_ERRORS = (TypeError, ValueError, ArithmeticError, struct.error)


def error_name(exc: BaseException) -> str:
    """Return the name of the exception class, e.g. ValueError or struct.error. Built-in names are not qualified."""
    cls = type(exc)
    return cls.__qualname__ if cls.__module__ == "builtins" else f"{cls.__module__}.{cls.__qualname__}"


# The callback configuration as returned by the devices. The Isolator Bricklet returns a SimpleCallbackConfiguration.
CallbackConfiguration: TypeAlias = AdvancedCallbackConfiguration | SimpleCallbackConfiguration


class TinkerforgeSensor:
    """This class extends the driver with catch-all arguments in the constructor"""

    @classmethod
    def driver(cls) -> str:
        """
        Returns
        -------
        str
            The driver name that identifies it to the sensor factory
        """
        return "tinkerforge2"

    @property
    def device(self) -> Device:
        """
        Returns
        -------
        Device
            The raw Tinkerforge sensor device
        """
        return self.__device

    def __init__(self, device: Device) -> None:
        self.__device = device
        self._logger = logging.getLogger(__name__)
        self.__topic = f"nodes/tinkerforge/{device.uid}"

    async def __aenter__(self) -> Self:
        event_bus.register(self.__topic + "/status", self.__get_sensor_status)
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        event_bus.unregister(self.__topic + "/status")

    async def __get_sensor_status(self) -> DeviceStatus:
        """
        Gather the device status and return it as a dataclass.
        Returns
        -------
        DeviceStatus
            A dataclass containing the device status
        """
        return DeviceStatus()

    @staticmethod
    def _stream_config_updates(sensor: TinkerforgeSensor) -> AsyncGenerator[dict[str, Any], None]:
        """
        Tries to fetch a config from the database. It also listens to 'nodes/tinkerforge/$UID/update' for new configs
        from the database.
        Parameters
        ----------
        sensor: TinkerforgeSensor
            The brick or bricklet for which to fetch a config from the database

        Returns
        -------
        AsyncGenerator of dict
            A dict containing the configuration of the device
        """
        return stream.chain(
            stream.call(
                call_safely,
                "db_tinkerforge_sensors/get_config",
                "db_tinkerforge_sensors/status_update",
                sensor.device.uid,
            )
            | pipe.takewhile(lambda config: config is not None),
            stream.iterate(event_bus.subscribe(f"nodes/tinkerforge/{sensor.device.uid}/update")),
        )

    def stream_data(self) -> AsyncGenerator[DataEvent, None]:
        """
        Generate the initial configuration of the sensor, configure it, and finally stream the data from the sensor.
        If there is a configuration update, reconfigure the sensor and start streaming again.
        Returns
        -------
        AsyncGenerator of DataEvent
            The data from the device
        """
        # Generates the first configuration
        # Query the database and if it does not have a config for the sensor, wait until there is one

        data_stream = (
            stream.chain(
                stream.just(self),
                stream.iterate(event_bus.subscribe(f"nodes/tinkerforge/{self.device.uid}/remove"))[:1]
                | pipe.map(lambda x: None),
            )
            | pipe.switchmap(
                lambda sensor: (
                    stream.empty()
                    if sensor is None
                    else (
                        self._stream_config_updates(sensor)
                        | pipe.switchmap(
                            lambda config: stream.chain(
                                # First inject the initial config into the stream,
                                stream.just(config),
                                # then listen for remove events and map those to None (no config) to unregister
                                # the sensor
                                stream.iterate(event_bus.subscribe(f"nodes/by_uuid/{config['uuid']}/remove"))[:1]
                                | pipe.map(lambda x: None),
                            )
                        )
                        | pipe.action(
                            lambda config: logging.getLogger(__name__).info(
                                "Got new configuration for: %s",
                                sensor.device,
                            )
                        )
                        | pipe.map(self._create_config)
                        | pipe.switchmap(
                            lambda config: (
                                stream.empty()
                                if config is None or not config["enabled"]
                                else (self._configure_and_stream(config))
                            )
                        )
                    )
                )
            )
            | context.pipe(self)
        )

        return data_stream

    def _create_config(self, config: dict[str, Any] | None) -> dict[str, tuple[partial, float] | Any] | None:
        """
        Maps the 'on_connect' key of the config to functions provided by the Tinkerforge device.
        Parameters
        ----------
        config: dict or None
            A dict that contains the key 'on_connect' which defines function calls to be made on the self.device object.
            If None is given, None will be returned.

        Returns
        -------
        dict or None
            A dict containing a key called 'on_connect' that holds a tuple[partial, float], with partials that run on
            the self.device object with a timeout in seconds (float).
            Returns None if the config input parameter is None as well.
        """
        if config is None:
            return None
        try:
            on_connect = tuple(create_device_function(self.device, func_call) for func_call in config["on_connect"])
            config["on_connect"] = on_connect
        except ConfigurationError:
            self._logger.error("Invalid configuration for %s: config=%s", self.device, config["on_connect"])
            return None

        return config

    def _read_sensor(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, source_uuid: UUID, sid: int, unit: str, topic: str, callback_config: CallbackConfiguration
    ) -> AsyncGenerator[DataEvent, None]:
        monitor_stream = (
            stream.repeat(self.device, interval=1)  # TODO: make interval configurable
            | pipe.map(async_(lambda sensor: sensor.get_callback_configuration(sid)))
            | pipe.map(lambda current_config: None if current_config == callback_config else self.device)
            | pipe.filter(lambda sensor: sensor is not None)
            | pipe.action(lambda sensor: logging.getLogger(__name__).info("Resetting callback config for %s", sensor))
            | pipe.action(async_(lambda sensor: self._apply_callback_configuration(sid, callback_config)))
            | pipe.filter(lambda x: False)
        )

        return stream.merge(
            stream.just(monitor_stream),
            stream.iterate(self.device.read_events(sids=(sid,)))
            | pipe.map(
                lambda item: DataEvent(
                    sender=source_uuid, topic=topic, value=item.payload, sid=item.sid, unit=str(unit)
                )
            ),
        )

    @staticmethod
    def _parse_callback_configuration(
        sid: str | int, config: dict[str, Any]
    ) -> tuple[int, str, str, AdvancedCallbackConfiguration]:
        """

        Parameters
        ----------
        sid: int or str
            The secondary id, naming the sensor of the node. Each node may have multiple sensors with a separate sid.
        config: dict
            A dictionary containing the keys 'interval' (in s), 'trigger_only_on_change', 'unit', and 'topic'

        Returns
        -------
        tuple of int, str, str, AdvancedCallbackConfiguration:
            The sid is an int specifying the secondary id of the sensor in the tree below the node
            The unit is a str naming the unit of measurement of this sensor
            The topic is a str defining where to publish the data
            The callback_config is an AdvancedCallbackConfiguration that specifies the configuration of this sensor
        """
        sid = int(sid)
        callback_config = AdvancedCallbackConfiguration(
            period=config["interval"],  # in s, like the database
            value_has_to_change=config["trigger_only_on_change"],
            option=ThresholdOption.OFF,
            minimum=None,
            maximum=None,
        )
        return sid, config["unit"], config["topic"], callback_config

    async def _apply_callback_configuration(self, sid: int, config: CallbackConfiguration) -> None:
        """
        Configure the callback of the sensor `sid`. The arguments are passed explicitly, because a configuration read
        from the device has no threshold option (None) if the channel has no thresholds, and the Isolator Bricklet
        returns a SimpleCallbackConfiguration without thresholds at all.

        Parameters
        ----------
        sid: int
            The secondary id of the sensor
        config: AdvancedCallbackConfiguration or SimpleCallbackConfiguration
            The callback configuration
        """
        if isinstance(config, SimpleCallbackConfiguration):
            await self.device.set_callback_configuration(sid, config.period, config.value_has_to_change)
            return
        await self.device.set_callback_configuration(
            sid,
            config.period,
            config.value_has_to_change,
            ThresholdOption.OFF if config.option is None else config.option,
            config.minimum,
            config.maximum,
        )

    async def _set_callback_configuration(self, sid: int, unit: str, topic: str, config: AdvancedCallbackConfiguration):
        try:
            await self._apply_callback_configuration(sid, config)
        except InvalidSidError as exc:
            # The sid does not exist or has no callback configuration. Disable only this sensor.
            self._logger.error("Invalid configuration: %s Sensor disabled. Config: %s", exc, config)
            return stream.empty()
        remote_callback_config: CallbackConfiguration
        remote_callback_config = await self.device.get_callback_configuration(sid)
        if remote_callback_config.period == 0:
            self._logger.warning(
                "Callback configuration for %s: sid=%i, config=%s failed. Source disabled.",
                self.device,
                sid,
                config,
            )
            return stream.empty()
        return stream.just((sid, unit, topic, remote_callback_config))

    async def _configure(self, config: TinkerforgeSensorConfigModel) -> bool:
        """
        Configure the sensor by calling all 'on_connect' functions set in the config, in order and one at a time. An
        invalid function call is logged and the remaining calls are skipped.

        Parameters
        ----------
        config: dict[str, tuple[partial, float]]
            A config dict that contains the key 'on_connect', which holds the function and a timeout when calling it.

        Returns
        -------
        bool
            True if all functions were called successfully, False if one of them was rejected as invalid.

        Raises
        ------
        TimeoutError
            If the device does not answer in time. This is a problem of the connection, not of the configuration.
        """
        func: partial
        for func, timeout in config["on_connect"]:
            try:
                await asyncio.wait_for(func(), timeout)
            except INVALID_CONFIGURATION_ERRORS as exc:
                arguments = [repr(arg) for arg in func.args] + [
                    f"{key}={value!r}" for key, value in func.keywords.items()
                ]
                self._logger.error(
                    "Invalid configuration for %s: on_connect call %s(%s) failed with %s: %s. Sensor disabled.",
                    self.device,
                    func.func.__name__,
                    ", ".join(arguments),
                    error_name(exc),
                    exc,
                )
                return False
        return True

    def _configure_and_stream(
        self,
        config: TinkerforgeSensorConfigModel | None,
    ) -> AsyncGenerator[DataEvent, None]:
        if config is None:
            return stream.empty()
        # Run all config steps first. If the configuration is invalid, the sensor is disabled. Otherwise, read all
        # sensors of the bricklet and process them in parallel (flatten).
        configured: Stream[bool] = stream.call(self._configure, config)
        return configured | pipe.switchmap(
            lambda is_configured: self._stream_sensors(config) if is_configured else stream.empty()
        )

    def _stream_sensors(self, config: TinkerforgeSensorConfigModel) -> Stream[DataEvent]:
        """Configure the callbacks of all sensors of the bricklet and stream their data in parallel."""
        return (
            stream.iterate(config["config"].items())
            | pipe.starmap(self._parse_callback_configuration)
            | pipe.starmap(self._set_callback_configuration)
            | pipe.flatten()
            | pipe.flatmap(lambda args: self._read_sensor(config["uuid"], *args))
        )
