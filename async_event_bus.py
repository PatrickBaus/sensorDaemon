"""
A lightweight event bus for the asyncio framework that relies on asynchronous
generators to deliver messages.
"""

from __future__ import annotations

import asyncio
import logging
from inspect import isasyncgen
from typing import Any, AsyncGenerator, Callable, Coroutine, cast


class EventRegisteredError(ValueError):
    """
    Raised if the event has already been registered by another handler
    """


class TopicNotRegisteredError(NameError):
    """
    Raised if the event was called but has not been registered
    """


class AsyncEventBus:
    """
    An event bus that is using the async generator syntax for distributing events.
    It uses dicts and sets internally to ensure good performance.
    """

    def __init__(self) -> None:
        self.__subscribers: dict[str, set[asyncio.Queue[Any]]] = {}
        self.__registered_calls: dict[str, Callable[[Any], Coroutine] | Callable[[Any], AsyncGenerator]] = {}
        self.__logger = logging.getLogger(__name__)

    async def subscribe(self, event_name: str) -> AsyncGenerator[Any, None]:
        """
        The async generator that yields events published for `event_name`.

        Parameters
        ----------
        event_name: str
            The type of event to listen for.

        Yields
        -------
        Any
            The events
        """
        self.__logger.debug("Subscribing to topic '%s'", event_name)
        queue: asyncio.Queue[Any] = asyncio.Queue()
        if self.__subscribers.get(event_name, None) is None:
            self.__subscribers[event_name] = {queue}
        else:
            self.__subscribers[event_name].add(queue)

        try:
            while "listening":
                event = await queue.get()
                yield event
        finally:
            # Cleanup
            self.__subscribers[event_name].remove(queue)
            if len(self.__subscribers[event_name]) == 0:
                del self.__subscribers[event_name]
            self.__logger.debug("Unsubscribed from topic '%s'", event_name)

    def publish(self, event_name: str, event: Any) -> None:
        """
        Publish an event called `event_name` with the payload `event`.

        Parameters
        ----------
        event_name: str
            The event address.
        event: any
            The data to be published.
        """
        self.__logger.debug("Publishing to topic '%s': %s", event_name, event)
        listener_queues: set[asyncio.Queue[Any]] = self.__subscribers.get(event_name, set())
        for queue in listener_queues:
            queue.put_nowait(event)

    def register(self, event_name: str, function: Callable[..., Coroutine] | Callable[..., AsyncGenerator]) -> None:
        """
        Register a function to be called via `call()`.

        Parameters
        ----------
        event_name: Any
            The type of event.
        function: Coroutine or AsyncGenerator
            A coroutine or async generator to be registered for calling.
        """
        if event_name in self.__registered_calls:
            raise EventRegisteredError(f"{event_name} is already registered")
        self.__logger.debug("Registering function as '%s'", event_name)
        self.__registered_calls[event_name] = function

    def unregister(self, event_name: str) -> None:
        """
        Unregister a previously registered function. Does not raise an error, if an unknown event is to be unregistered.

        Parameters
        ----------
        event_name: Any
            The name of event to be unregistered.
        """
        self.__logger.debug("Unregistering function call from '%s'", event_name)
        self.__registered_calls.pop(event_name, None)

    async def call(self, event_name: str, *args, ignore_unregistered: bool = False, **kwargs) -> Any:
        """
        Call a registered function.

        Parameters
        ----------
        event_name: str
            The name of the of event to be called.
        ignore_unregistered: bool
            If True, do not raise an error if the event_name has not been registered
        args: List
            The arguments to be passed to the function called.
        kwargs: Dict
            The keyword arguments to be passed to the function called.

        Raises
        ------
        TopicNotRegisteredError
            Raised if the function `event_name` is not registered.
        """
        self.__logger.debug("Calling function '%s' with args: %s, kwargs: %s", event_name, args, kwargs)
        try:
            gen_or_func: Coroutine | AsyncGenerator = self.__registered_calls[event_name](*args, **kwargs)
            if isasyncgen(gen_or_func):
                return gen_or_func
            return await cast(Coroutine, gen_or_func)
        except KeyError:
            if not ignore_unregistered:
                raise TopicNotRegisteredError(f"Function {event_name} is not registered.") from None


event_bus = AsyncEventBus()
