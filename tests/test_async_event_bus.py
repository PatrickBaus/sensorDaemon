"""Unit tests for the asynchronous event bus."""

import pytest

from async_event_bus import AsyncEventBus, TopicNotRegisteredError


@pytest.mark.asyncio
async def test_call_raises_for_unregistered_topic():
    """Calling an unknown topic raises TopicNotRegisteredError."""
    event_bus = AsyncEventBus()

    with pytest.raises(TopicNotRegisteredError):
        await event_bus.call("unknown/topic")


@pytest.mark.asyncio
async def test_call_ignores_unregistered_topic_if_requested():
    """With ignore_unregistered=True, calling an unknown topic returns None."""
    event_bus = AsyncEventBus()

    assert await event_bus.call("unknown/topic", ignore_unregistered=True) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("ignore_unregistered", [False, True])
async def test_call_passes_on_key_errors_of_the_function(ignore_unregistered):
    """A KeyError raised by the registered function is passed on and not mistaken for an unregistered topic."""
    event_bus = AsyncEventBus()

    async def get_config(uid):
        return {}[uid]

    event_bus.register("test/get_config", get_config)

    with pytest.raises(KeyError):
        await event_bus.call("test/get_config", 42, ignore_unregistered=ignore_unregistered)


@pytest.mark.asyncio
async def test_call_returns_async_generators_without_awaiting_them():
    """A registered async generator is returned to the caller to iterate."""
    event_bus = AsyncEventBus()

    async def get_all():
        yield 1
        yield 2

    event_bus.register("test/get", get_all)

    assert [item async for item in await event_bus.call("test/get")] == [1, 2]
