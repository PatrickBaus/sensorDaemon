"""
This is an asyncIO driver for a generic SCPI compatible device.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from functools import partial
from typing import TYPE_CHECKING

from aiostream import pipe, stream
from aiostream.aiter_utils import is_async_iterable
from hp3478a_async import HP_3478A

from async_event_bus import event_bus
from data_types import DataEvent
from helper_functions import create_device_function, finally_action
from errors import ConfigurationError


class GenericDriver:
    """This class extends a driver with catch-all arguments in the constructor"""

    def __init__(
            self,
            uuid,
            *args,
            **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.__uuid = uuid

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
        on_read, timeout = config['on_read']
        if inspect.isasyncgenfunction(on_read.func):
            return stream.iterate(on_read()) | pipe.timeout(timeout)
        else:
            return (
                    stream.repeat(config['on_read'], interval=config['interval'])
                    | pipe.starmap(lambda func, timeout: stream.just(func()) | pipe.timeout(timeout))
                    | pipe.concat()
            )

    def _configure_and_stream(self, config):
        if config is None:
            return stream.empty()
        # Run all config steps in order (concat) and one at a time (task_limit=1). Drop the output. There is nothing to
        # compare them to (filter => false), then read the device.
        config_stream = (
            stream.chain(
                stream.iterate(config['on_connect'])
                | pipe.starmap(lambda func, timeout: stream.just(func()) | pipe.timeout(timeout))
                | pipe.concat(task_limit=1)
                | pipe.filter(lambda result: False),
                self._read_device(config)
                | pipe.map(
                    lambda item: DataEvent(
                        sender=config['uuid'], topic=config['topic'], value=item, sid=0, unit=config['unit']
                    )
                )
                | finally_action.pipe(stream.call(self._clean_up, config['on_disconnect']))
            )
        )
        return config_stream

    def _parse_config(self, config):
        if config is None:
            return None
        try:
            config['on_connect'] = tuple(create_device_function(self, func_call) for func_call in config['on_connect'])
            config['on_disconnect'] = tuple(
                create_device_function(self, func_call) for func_call in config['on_disconnect']
            )
            config['on_read'] = create_device_function(self, config['on_read'])
            config['on_after_read'] = tuple(
                create_device_function(self, func_call) for func_call in config['on_after_read']
            )
        except ConfigurationError:
            config = None

        return config

    def stream_data(self, config):
        data_stream = (
            stream.chain(
                stream.just(config),
                stream.iterate(event_bus.subscribe(f"nodes/by_uuid/{self.__uuid}/update"))
            )
            | pipe.action(
                lambda config: logging.getLogger(__name__).info(
                    "Got new configuration for: %s => %s", self, config
                ) if config is not None else logging.getLogger(__name__).info(
                    "Removed configuration for: %s", self
                )
            )
            | pipe.map(self._parse_config)
            | pipe.switchmap(
                lambda conf: stream.empty() if conf is None or not conf['enabled'] else (
                    self._configure_and_stream(conf)
                )
            )
        )

        return data_stream
