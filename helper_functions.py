# -*- coding: utf-8 -*-
"""
A collection of helper functions used in Kraken.
"""
import asyncio


async def cancel_all_tasks(tasks):
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
        if isinstance(result, Exception):
            raise result
