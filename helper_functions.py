"""
A collection of helper functions used in Kraken.
"""
from __future__ import annotations

import asyncio
import inspect
from functools import partial
from typing import Any, AsyncContextManager, AsyncGenerator, AsyncIterable, Awaitable, Callable, Type, TypedDict, cast

from aiostream import operator, stream, streamcontext
from aiostream.core import Stream

from async_event_bus import TopicNotRegisteredError, event_bus
from errors import ConfigurationError


class FunctionCallConfig(TypedDict):
    """This is the datatype for a function call on a device as it is stored in the database."""

    function: str
    args: list | tuple
    kwargs: dict
    timeout: float


async def cancel_all_tasks(tasks: set[asyncio.Task]) -> None:
    """
    Cancels all tasks and waits for them to finish. It then tests the results
    for exceptions (except asyncio.CancelledError) and raises the first one found.
    Parameters
        ----------
        tasks: Iterable
            The tasks to be cancelled and awaited
    """
    for task in tasks:
        if not task.done():
            task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        # Check for exceptions, but ignore asyncio.CancelledError, which inherits from BaseException not Exception
        if isinstance(result, Exception):
            raise result


async def iterate_safely(topic: str, status_topic: str, *args: Any, **kwargs: Any) -> AsyncGenerator[Any, None]:
    """
    Iterate over a topic on the eventbus. If the topic is not yet registered, register at the status topic and wait for
    the data source to become available.

    Parameters
    ----------
    topic: str
        The data source topic
    status_topic:
        The data source status update topic, that will be listened to, if the source is not available
    *args; Any
        The arguments passed to the eventbus topic
    **kwargs: Any
        The keyword arguments passed to the eventbus topic

    Yields
    -------
    Any
        The data returned from the subscription

    """
    while "database not ready":
        try:
            gen = await event_bus.call(topic, *args, **kwargs)
        except NameError:
            # The database is not yet ready, wait for it
            status: bool  # TODO: Replace with proper event
            async for status in event_bus.subscribe(status_topic):
                if status:
                    break
            continue
        else:
            async for item in gen:
                yield item
        break


async def call_safely(topic: str, status_topic: str, *args: Any, **kwargs: Any) -> Any:
    """
    Call a topic on the eventbus. If the topic is not yet registered, register at the status topic and wait for
    the data source to become available.
    Parameters
    ----------
    topic: str
        The data source topic
    status_topic:
        The data source status update topic, that will be listened to, if the source is not available
    *args; Any
        The arguments passed to the eventbus topic
    **kwargs: Any
        The keyword arguments passed to the eventbus topic

    Returns
    -------
    Any
        The result of the event_bus function call.
    """
    while "database not ready":
        try:
            result = await event_bus.call(topic, *args, **kwargs)
        except TopicNotRegisteredError:
            # The database is not yet ready, wait for it
            status: bool  # TODO: Replace with proper event
            async for status in event_bus.subscribe(status_topic):
                if status:
                    break
            continue
        else:
            return result


@operator(pipable=True)
async def retry(
    source: AsyncIterable[Any], exc_class: Type[BaseException], interval: float = 0
) -> AsyncGenerator[Any, None]:
    """
    Retry a datastream if the exception `exc_class` is thrown.

    Parameters
    ----------
    source: AsyncIterable
    exc_class: BaseException type
        The exception class to catch
    interval: float
        The time in seconds to wait between retries

    Yield
    -------
    Any
        The results from the stream
    """
    timeout: float = 0
    loop = asyncio.get_event_loop()
    while True:
        try:
            async with streamcontext(source) as streamer:
                async for item in streamer:
                    yield item
        except exc_class:
            delay = timeout - loop.time()
            await asyncio.sleep(delay)
            timeout = loop.time() + interval
            continue
        else:
            return


@operator(pipable=True)
async def context(
    source: AsyncIterable[Any],
    context_manager: AsyncContextManager,
    on_enter: Callable[[], Any] | None = None,
    on_exit: Callable[[], Any] | None = None,
) -> AsyncGenerator[Any, None]:
    """
    Iterate a stream within a context. The on_enter and on_exit callbacks can be used to log the status of the stream.
    Parameters
    ----------
    source: AsyncIterable
    context_manager: AsyncContextManager
        The asynchronous context manager that needs to be entered
    on_enter: Callable
        A function to be called once the context has been entered
    on_exit: Callable
        A function to be called once the context is left.

    Yields
    -------
    Any
        The results from the data stream
    """
    async with context_manager:
        async with streamcontext(source) as streamer:
            try:
                if on_enter is not None:
                    on_enter()
                    async for item in streamer:
                        yield item
            finally:
                if on_exit is not None:
                    on_exit()


@operator
async def with_context(
    context_manager: AsyncContextManager, on_exit: Callable[[], Any] | None = None
) -> AsyncGenerator[Any, None]:
    """
    Enters the context and yields the context.
    Parameters
    ----------
    context_manager: AsyncContextManager
        The context manager to enter
    on_exit: Callable
        A callback function to call, when exiting the context

    Yields
    -------
    Any
        The context
    """
    try:
        async with context_manager as ctx:
            yield ctx
            future: asyncio.Future = asyncio.Future()
            try:
                await future
            finally:
                future.cancel()
    finally:
        if on_exit is not None:
            on_exit()


@operator(pipable=True)
async def finally_action(
    source: AsyncIterable[Any], func: Awaitable[Any] | Callable[[], Any]
) -> AsyncGenerator[Any, None]:
    """
    Wrap a try/finally around a stream context.

    Parameters
    ----------
    source: AsyncIterable
    func: Awaitable or Callable
        The function to be called when the entering the finally-block.

    Yields
    -------
    Any
        The results from the source stream
    """
    try:
        async with streamcontext(source) as streamer:
            async for item in streamer:
                yield item
    finally:
        if inspect.isawaitable(func):
            await func
        else:
            cast(Callable, func)()


@operator(pipable=True)
async def catch(
    source: AsyncIterable[Any], exc_class: Type[BaseException], on_exc: Callable[[BaseException], Stream] | None = None
) -> AsyncGenerator[Any, None]:
    """
    Catch an exception and then switch to the next stream `on_exc` or gracefully terminate, when no stream is given.
    Parameters
    ----------
    source: AsyncIterable
    exc_class: BaseException type
        The exception to catch
    on_exc: Callable
        A function, that takes an exception and returns a Stream.

    Yields
    -------
    Any
        The results from the source stream or the `on_exc` stream.
    """
    try:
        async with streamcontext(source) as streamer:
            async for item in streamer:
                yield item
    except exc_class as exc:
        if on_exc is not None:
            async with on_exc(exc).stream() as streamer:
                async for item in streamer:
                    yield item
        else:
            yield stream.empty()


def create_device_function(device: Any, func_call: FunctionCallConfig) -> tuple[partial, float]:
    """
    Creates a partial function from the function call with the parameters given and returns it
    Parameters
    ----------
    device: Any
    func_call: dict
        a dictionary containing the function call as string and the optional args and kwargs parameters
    Returns
    -------
    tuple of partial and float
        The function call and the timeout
    """
    try:
        function = getattr(device, func_call["function"])
        # Create a partial function, that freezes the parameters and can be called later
        func = partial(function, *func_call.get("args", []), **func_call.get("kwargs", {}))
        timeout = func_call["timeout"]
    except AttributeError:
        raise ConfigurationError(f"Function '{func_call['function']}' not found") from None
    return func, timeout
