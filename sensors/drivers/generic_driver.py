"""
This is an asyncIO driver for a generic SCPI compatible device.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from functools import partial
from typing import Any, AsyncGenerator, AsyncIterable, Iterable

from aiostream import pipable_operator, pipe, stream, streamcontext

from async_event_bus import event_bus
from data_types import DataEvent
from errors import ConfigurationError
from helper_functions import catch, create_device_function, finally_action, retry, timeout_with_reason


@pipable_operator
async def call_after_each(source: AsyncIterable[Any], calls: AsyncIterable[Any]) -> AsyncGenerator[Any, None]:
    """
    Forward the items of a stream and iterate `calls` after each item, before the next item is requested from the
    source.

    Parameters
    ----------
    source: AsyncIterable
        The source stream
    calls: AsyncIterable
        A stream, that is iterated (and its results dropped) after each item of the source

    Yields
    -------
    Any
        The items of the source stream
    """
    async with streamcontext(source) as streamer:
        async for item in streamer:
            yield item
            async with streamcontext(calls) as calls_streamer:
                async for _ in calls_streamer:
                    pass


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

    def _call_functions(self, funcs: Iterable[tuple[partial, float]]):
        """
        Call the functions one after another, each with its own timeout, and drop their results.

        Parameters
        ----------
        funcs: Iterable of tuple of partial and float
            The functions to call and their timeouts in seconds

        Returns
        -------
        Stream
            An empty stream, that calls the functions when iterated.
        """
        return (
            stream.iterate(funcs)
            | pipe.starmap(lambda func, timeout: stream.just(func()) | self._timeout(func, timeout))
            | pipe.concat(task_limit=1)
            | pipe.filter(lambda result: False)
        )

    def _timeout(self, func: partial, timeout: float | None):
        """
        A timeout for a call to the device. When it expires, the TimeoutError names the device, the function and the
        timeout, which are logged when the connection is closed.

        Parameters
        ----------
        func: partial
            The function called
        timeout: float or None
            The timeout in seconds from the config

        Returns
        -------
        Callable
            The pipable timeout operator
        """
        return timeout_with_reason.pipe(
            timeout, lambda: f"{self}: {func.func.__name__}() got no reply within the timeout of {timeout} s"
        )

    def _with_after_read(self, read_stream, config: dict[str, Any]):
        """
        Call the 'on_after_read' functions of the config after the data of a read has been streamed.

        Parameters
        ----------
        read_stream: Stream
            The data of a single read
        config: dict
            The device config, that may contain the key 'on_after_read'

        Returns
        -------
        Stream
            The data of the read followed by the calls to the 'on_after_read' functions
        """
        after_read = config.get("on_after_read")
        if not after_read:
            return read_stream
        return stream.chain(read_stream, self._call_functions(after_read))

    def _read_device(self, config: dict[str, Any]) -> AsyncGenerator[tuple[int, Any], None]:
        on_read: partial
        timeout: float
        on_read, timeout = config["on_read"]
        if inspect.isasyncgenfunction(on_read.func):
            data_stream = (
                stream.iterate(on_read()) | pipe.map(lambda value: (0, value)) | self._timeout(on_read, timeout)
            )
            if config.get("on_after_read"):
                # Call the 'on_after_read' functions after each value produced by the generator
                data_stream = data_stream | call_after_each.pipe(self._call_functions(config["on_after_read"]))
            return data_stream | retry.pipe(
                (ValueError,), config["interval"], action=lambda exc: self.log_error(exc, msg="Retrying.")
            )
        return (
            stream.repeat(config["on_read"], interval=config["interval"])  # Repeat query to on_read at interval
            | pipe.starmap(
                lambda func, timeout: self._with_after_read(
                    stream.just(func())  # Get the results of the query (a mapping/list)
                    | pipe.concatmap(stream.iterate)  # iterate the results
                    | pipe.enumerate()  # add the sid for each result in order
                    | self._timeout(func, timeout),  # time out if no result is produced within the deadline
                    config,
                )
            )
            | pipe.concat(task_limit=1)
            | retry.pipe((ValueError,), config["interval"], action=lambda exc: self.log_error(exc, msg="Retrying."))
        )

    def on_error(self, exc: BaseException) -> AsyncGenerator[None, None]:
        """
        The function to call in case of an execution during streaming.

        Parameters
        ----------
        exc: BaseException
            The exception, that was raised

        Returns
        -------
        AsyncGenerator
            Am empty stream, that terminates without generating a value.
        """
        self.log_error(exc, "Terminating device.")
        return stream.empty()

    def log_error(self, exc: BaseException, msg: str | None = None):
        """
        Log an error that occurred while reading from the device.

        Parameters
        ----------
        exc : BaseException
            The exception that caused the error.
        msg : str, optional
            Additional information to include in the log message.
            If ``None`` or an empty string, no additional message is logged.
        """
        if msg:
            logging.getLogger(__name__).error("Error while reading %s. Error: %s. %s", self, exc, msg)
        else:
            logging.getLogger(__name__).error("Error while reading %s. Error: %s", self, exc)

    def _configure_and_stream(self, config: dict[str, Any]) -> AsyncGenerator[DataEvent, None]:
        if config is None:
            return stream.empty()
        # Run all config steps in order (concat) and one at a time (task_limit=1). Drop the output. There is nothing to
        # compare them to (filter => false), then read the device.
        config_stream = stream.chain(
            stream.iterate(config["on_connect"])
            | pipe.starmap(lambda func, timeout: stream.just(func()) | self._timeout(func, timeout))
            | pipe.concat(task_limit=1)
            | pipe.filter(lambda result: False),
            self._read_device(config)
            | catch.pipe(ValueError, on_exc=self.on_error)
            | pipe.starmap(
                lambda sid, item: DataEvent(
                    sender=config["uuid"], topic=config["topic"], value=item, sid=sid, unit=config["unit"]
                )
            )
            | finally_action.pipe(stream.call(self._clean_up, config["on_disconnect"])),
        ) | catch.pipe(TypeError, on_exc=self.on_error)

        return config_stream

    def _parse_config(self, config: dict[str, Any]) -> dict[str, Any] | None:
        """
        Takes a config and parses it to function calls. If there are errors, do not return the config.
        Parameters
        ----------
        config: dict
            The config to be parsed

        Returns
        -------
        dict or None
            Either returns the parsed config or None if there were errors.
        """
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
            logging.getLogger(__name__).error("Invalid configuration for %s: config=%s", self, config)
            return None

        return config

    def _log_config_progress(self, config: dict[str, Any] | None):
        if config is None:
            logging.getLogger(__name__).info("Invalid configuration for: %s.", self)
        else:
            if config["enabled"]:
                logging.getLogger(__name__).info("Enabling device: %s.", self)
            else:
                logging.getLogger(__name__).info("Disabling device: %s.", self)

    def stream_data(self, initial_config: dict[str, Any]) -> AsyncGenerator[DataEvent, None]:
        """
        Stream the data from the sensor.
        Parameters
        ----------
        initial_config: dict
            A dictionary containing the initial sensor configuration.

        Returns
        -------
        AsyncGenerator
            The asynchronous stream.
        """
        data_stream = (
            stream.chain(
                stream.just(initial_config), stream.iterate(event_bus.subscribe(f"nodes/by_uuid/{self.__uuid}/update"))
            )
            | pipe.action(
                lambda config: (
                    logging.getLogger(__name__).info("Got new configuration for: %s.", self)
                    if config is not None
                    else logging.getLogger(__name__).info("Removed configuration for: %s.", self)
                )
            )
            | pipe.map(self._parse_config)
            | pipe.action(self._log_config_progress)
            | pipe.switchmap(
                lambda config: (
                    stream.empty() if config is None or not config["enabled"] else (self._configure_and_stream(config))
                )
            )
        )

        return data_stream
