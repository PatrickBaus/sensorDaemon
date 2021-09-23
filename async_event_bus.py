# -*- coding: utf-8 -*-
"""
A lightweight event bus for the AsyncIO framework, that relies on asynchronous
generators to deliver messages.
"""
import asyncio

from inspect import isasyncgen


class AsyncEventBus():
    """
    An event bus, that is using the async generator syntax for distributing events.
    It uses dicts and sets internally to ensure good performance.
    """
    def __init__(self):
        self.__subscribers = {}
        self.__registered_calls = {}

    async def subscribe(self, event_name):
        """
        The async generator, that yields events subscribed to `event_name`.

        Parameters
        ----------
        event_name: Any
            The type of event to listen for. Typically a str, but can be anything.

        Returns
        -------
        AsyncIterator[Any]
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

    def publish(self, event_name, event):
        """
        Publish an event called `event_name` with the payload `event`.

        Parameters
        ----------
        event_name: Any
            The type of event.
        event: any
            The data to be published.
        """
        listener_queues = self.__subscribers.get(event_name, [])
        for queue in listener_queues:
            queue.put_nowait(event)

    def register(self, event_name, function):
        """
        Register a function to be called via `call()`.

        Parameters
        ----------
        event_name: Any
            The type of event.
        function: CoroutineType or AsyncGeneratorType
            A coroutine or async generator.
        """
        self.__registered_calls[event_name] = function

    def unregister(self, event_name):
        """
        Unregister a previously registered function.

        Parameters
        ----------
        event_name: Any
            The type of event.
        """
        self.__registered_calls.pop(event_name, None)

    async def call(self, event_name, *args, ignore_unregistered=False, **kwargs):
        """
        Call a registered function.

        Parameters
        ----------
        event_name: Any
            The type of event.
        ignore_unregistered: bool
            Do not raise an error if True and the call is not registered
        args: List
            The arguments to be passed to the function called.
        kwargs: Dict
            The keyword arguments to be passed to the function called.
        """
        try:
            gen_or_func = self.__registered_calls[event_name](*args, **kwargs)
            if isasyncgen(gen_or_func):
                return gen_or_func
            return await gen_or_func
        except KeyError:
            if not ignore_unregistered:
                raise NameError(f"Event {event_name} is not registered.") from None
