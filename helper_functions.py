"""
A collection of helper functions used in Kraken.
"""
import asyncio
import inspect
from functools import partial
from typing import Any, AsyncGenerator, Union

from aiostream import operator, stream, streamcontext

from async_event_bus import TopicNotRegisteredError, event_bus
from errors import ConfigurationError


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


async def iterate_safely(
        topic: str,
        status_topic: str,
        *args: Any,
        **kwargs: Any
) -> AsyncGenerator[Any, None]:
    while "database not ready":
        try:
            gen = await event_bus.call(topic, *args, **kwargs)
        except NameError:
            # The database is not yet ready, wait for it
            status: bool    # TODO: Replace with proper event
            async for status in event_bus.subscribe(status_topic):
                if status:
                    break
            continue
        else:
            async for item in gen:
                yield item
        break


@operator(pipable=True)
async def retry(source, exc_class: Exception = Exception, interval: float = 0):
    timeout = 0
    loop = asyncio.get_event_loop()
    while True:
        try:
            async with streamcontext(source) as streamer:
                async for item in streamer:
                    yield item
        except exc_class as exc:
            print(f"caught exception of type {type(exc)}")
            delay = timeout - loop.time()
            await asyncio.sleep(delay)
            timeout = loop.time() + interval
            continue
        else:
            return


@operator(pipable=True)
async def context(source, cm, on_enter=None, on_exit=None):
    async with cm:
        try:
            if on_enter is not None:
                on_enter()
            async with streamcontext(source) as streamer:
                async for item in streamer:
                    yield item
            #yield in_context
        finally:
            if on_exit is not None:
                on_exit()


@operator(pipable=True)
async def finally_action(source, func):
    try:
        async with streamcontext(source) as streamer:
            async for item in streamer:
                yield item
    finally:
        if inspect.isawaitable(func):
            await func
        else:
            func()


@operator(pipable=True)
async def catch(source, exc_class: Exception, on_exc=None):
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

async def call_safely(
        topic: str,
        status_topic: str,
        *args: Any,
        **kwargs: Any
):
    while "database not ready":
        try:
            result = await event_bus.call(topic, *args, **kwargs)
        except TopicNotRegisteredError:
            # The database is not yet ready, wait for it
            status: bool    # TODO: Replace with proper event
            async for status in event_bus.subscribe(status_topic):
                if status:
                    break
            continue
        else:
            return result
        break


def create_device_function(
        device: Any,
        func_call: dict[str, Union[str, tuple[Any, ...], dict[str, Any]]]
) -> partial:
    """
    Creates a partial function from the function call with the parameters given and returns it
    Parameters
    ----------
    device: Any
    func_call: dict
        a dictionary containing the function call as string and the optional args and kwargs parameters
    Returns
    -------
    partial
        The function call
    """
    try:
        function = getattr(device, func_call['function'])
        # Create a partial function, that freezes the parameters and can be called later
        func = partial(function, *func_call.get('args', []), **func_call.get('kwargs', {})), func_call['timeout']
    except AttributeError:
        raise ConfigurationError(
            f"Function '{func_call['function']}' not found"
        ) from None
    return func
