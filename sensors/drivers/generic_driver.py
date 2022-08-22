"""
This is an asyncIO driver for a generic SCPI compatible device.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from functools import partial
from typing import Any

from aiostream import pipe, stream

from async_event_bus import event_bus
from data_types import DataEvent
from errors import ConfigurationError
from helper_functions import catch, create_device_function, finally_action


class GenericDriverMixin:
    """This mixin adds the streaming interface to a driver class."""

    def __init__(self, uuid, *args: Any, **kwargs: Any) -> None:
        self.__uuid = uuid
        # Call the base class constructor, because this is just a mixin, that comes before the base class in the MRO,
        # so there *might* be a base class.
        super().__init__(*args, **kwargs)

    async def _clean_up(self, funcs):
        results = await asyncio.gather(
            *[asyncio.wait_for(func(), timeout) for func, timeout in funcs], return_exceptions=True
        )
        for result in results:
            if isinstance(result, Exception):
                logging.getLogger(__name__).error("Error during shutdown of: %s", self, exc_info=result)

    @staticmethod
    def _read_device(config):
        on_read: partial
        timeout: float
        on_read, timeout = config["on_read"]
        if inspect.isasyncgenfunction(on_read.func):
            return stream.iterate(on_read()) | pipe.timeout(timeout)
        else:
            return (
                stream.repeat(config["on_read"], interval=config["interval"])
                | pipe.starmap(lambda func, timeout: stream.just(func()) | pipe.timeout(timeout))
                | pipe.concat(task_limit=1)
            )

    def on_error(self, exc):
        logging.getLogger(__name__).error("Error while while reading %s. Terminating device. Error: %s", self, exc)
        return stream.empty()

    def _configure_and_stream(self, config):
        if config is None:
            return stream.empty()
        # Run all config steps in order (concat) and one at a time (task_limit=1). Drop the output. There is nothing to
        # compare them to (filter => false), then read the device.
        config_stream = stream.chain(
            stream.iterate(config["on_connect"])
            | pipe.starmap(lambda func, timeout: stream.just(func()) | pipe.timeout(timeout))
            | pipe.concat(task_limit=1)
            | pipe.filter(lambda result: False),
            self._read_device(config)
            | pipe.map(
                lambda item: DataEvent(
                    sender=config["uuid"], topic=config["topic"], value=item, sid=0, unit=config["unit"]
                )
            )
            | finally_action.pipe(stream.call(self._clean_up, config["on_disconnect"])),
        ) | catch.pipe(TypeError, on_exc=self.on_error)
        return config_stream

    def _parse_config(self, config):
        if config is None:
            return None
        try:
            config["on_connect"] = tuple(create_device_function(self, func_call) for func_call in config["on_connect"])
            config["on_disconnect"] = tuple(
                create_device_function(self, func_call) for func_call in config["on_disconnect"]
            )
            config["on_read"] = create_device_function(self, config["on_read"])
            config["on_after_read"] = tuple(
                create_device_function(self, func_call) for func_call in config["on_after_read"]
            )
        except ConfigurationError:
            config = None

        return config

    def stream_data(self, config):
        data_stream = (
            stream.chain(
                stream.just(config), stream.iterate(event_bus.subscribe(f"nodes/by_uuid/{self.__uuid}/update"))
            )
            | pipe.action(
                lambda config: logging.getLogger(__name__).info("Got new configuration for: %s", self)
                if config is not None
                else logging.getLogger(__name__).info("Removed configuration for: %s", self)
            )
            | pipe.map(self._parse_config)
            | pipe.switchmap(
                lambda conf: stream.empty()
                if conf is None or not conf["enabled"]
                else (self._configure_and_stream(conf))
            )
        )

        return data_stream
