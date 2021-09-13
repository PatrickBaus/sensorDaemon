# -*- coding: utf-8 -*-
"""
A lightweight event bus for the AsyncIO framework, that relies on asynchronous
generators to deliver messages.
"""
import asyncio


class AsyncEventBus():
    """
    An event bus, that is using the async generator syntax for distributing events.
    It uses dicts and sets internally to ensure good performance.
    """
    def __init__(self):
        self.__listeners = {}

    async def register(self, event_name):
        """
        The async generator, that yield events registered to `event_name`.

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
        if self.__listeners.get(event_name, None) is None:
            self.__listeners[event_name] = {queue}
        else:
            self.__listeners[event_name].add(queue)

        try:
            while "listening":
                event = await queue.get()
                yield event
        finally:
            # Cleanup
            self.__listeners[event_name].remove(queue)
            if len(self.__listeners[event_name]) == 0:
                del self.__listeners[event_name]

    def emit(self, event_name, event):
        """
        Emit an event called `event_name` with the payload `event`.

        Parameters
        ----------
        event_name: Any
            The type of event.
        event: any
            The data to be published.
        """
        listener_queues = self.__listeners.get(event_name, [])
        for queue in listener_queues:
            queue.put_nowait(event)
