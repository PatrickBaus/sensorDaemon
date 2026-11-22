"""Unit tests for the asynchronous event bus."""

import asyncio

import pytest

from async_event_bus import AsyncEventBus, EventRegisteredError, TopicNotRegisteredError


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


@pytest.mark.asyncio
async def test_published_events_reach_all_subscribers():
    """Every subscriber of a topic receives every event published after it subscribed."""
    event_bus = AsyncEventBus()
    first = event_bus.subscribe("test/topic")
    second = event_bus.subscribe("test/topic")
    # A subscription is registered when the generator starts, i.e. when the first event is requested
    first_event = asyncio.ensure_future(anext(first))
    second_event = asyncio.ensure_future(anext(second))
    await asyncio.sleep(0)

    event_bus.publish("test/topic", 42)
    event_bus.publish("other/topic", 0)  # not delivered to the subscribers of test/topic

    assert await first_event == 42
    assert await second_event == 42
    await first.aclose()
    await second.aclose()


@pytest.mark.asyncio
async def test_unsubscribing_removes_the_topic_when_the_last_subscriber_leaves():
    """A closed subscription no longer receives events. Publishing to a topic without subscribers is a no-op."""
    event_bus = AsyncEventBus()
    first = event_bus.subscribe("test/topic")
    second = event_bus.subscribe("test/topic")
    pending_first = asyncio.ensure_future(anext(first))
    pending_second = asyncio.ensure_future(anext(second))
    await asyncio.sleep(0)

    pending_first.cancel()
    pending_second.cancel()
    await asyncio.sleep(0)
    await first.aclose()  # one subscriber left
    event_bus.publish("test/topic", 1)
    await second.aclose()  # no subscriber left
    event_bus.publish("test/topic", 2)

    assert event_bus._AsyncEventBus__subscribers == {}  # pylint: disable=protected-access


def test_a_topic_can_only_be_registered_once():
    """Registering a second function for the same topic raises an EventRegisteredError until it is unregistered."""
    event_bus = AsyncEventBus()

    async def function():
        """Provide a test helper."""

    event_bus.register("test/function", function)
    with pytest.raises(EventRegisteredError):
        event_bus.register("test/function", function)
    event_bus.unregister("test/function")
    event_bus.unregister("test/function")  # unregistering an unknown topic is not an error
    event_bus.register("test/function", function)
