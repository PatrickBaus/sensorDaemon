"""
A lightweight event bus for the AsyncIO framework, that relies on asynchronous
generators to deliver messages.
"""
import asyncio
from inspect import isasyncgen
from typing import Any, AsyncGenerator, Callable, Coroutine, Union


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
    An event bus, that is using the async generator syntax for distributing events.
    It uses dicts and sets internally to ensure good performance.
    """
    def __init__(self):
        self.__subscribers: dict[str, set[asyncio.Queue]] = {}
        self.__registered_calls: dict[str, Union[Callable[[Any], Coroutine], Callable[[Any], AsyncGenerator]]] = {}

    async def subscribe(self, event_name: str) -> AsyncGenerator[Any, None]:
        """
        The async generator, that yields events subscribed to `event_name`.

        Parameters
        ----------
        event_name: str
            The type of event to listen for, typically a str, but can be anything.

        Yields
        -------
        Any
            The events
        """
        queue = asyncio.Queue()
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
        listener_queues = self.__subscribers.get(event_name, [])
        for queue in listener_queues:
            queue.put_nowait(event)

    def register(self, event_name: str, function: Union[Callable[[Any], Coroutine], Callable[[Any], AsyncGenerator]]) -> None:
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
        self.__registered_calls[event_name] = function

    def unregister(self, event_name: str) -> None:
        """
        Unregister a previously registered function. Does not raise an error, if an unknown event is to be unregistered.

        Parameters
        ----------
        event_name: Any
            The name of event to be unregistered.
        """
        self.__registered_calls.pop(event_name, None)

    async def call(self, event_name: str, *args, ignore_unregistered: bool = False, **kwargs) -> Any:
        """
        Call a registered function.

        Parameters
        ----------
        event_name: str
            The name of the of event to be called.
        ignore_unregistered: bool
            Do not raise an error if True and the call is not registered
        args: List
            The arguments to be passed to the function called.
        kwargs: Dict
            The keyword arguments to be passed to the function called.

        Raises
        ------
        NameError
            Raised if the function `event_name` is not registered.
        """
        try:
            gen_or_func = self.__registered_calls[event_name](*args, **kwargs)
            if isasyncgen(gen_or_func):
                return gen_or_func
            return await gen_or_func
        except KeyError:
            if not ignore_unregistered:
                raise TopicNotRegisteredError(f"Function {event_name} is not registered.") from None


event_bus = AsyncEventBus()
