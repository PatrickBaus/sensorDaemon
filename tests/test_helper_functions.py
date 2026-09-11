"""Unit tests for helper functions used by the sensor daemon."""

# pylint: disable=protected-access,redefined-outer-name,too-few-public-methods,too-many-public-methods

import asyncio
from functools import partial
from typing import AsyncIterator
from unittest.mock import AsyncMock, Mock

import pytest
from aiostream import stream

import helper_functions
from async_event_bus import TopicNotRegisteredError
from errors import ConfigurationError


class AsyncIterable:
    """Reusable asynchronous iterable backed by a factory."""

    def __init__(self, factory):
        self.factory = factory

    def __aiter__(self):
        return self.factory()


class AsyncContext:
    """Minimal asynchronous context manager for stream tests."""

    def __init__(self, value=None, enter_error=None, exit_error=None):
        self.value = value
        self.enter_error = enter_error
        self.exit_error = exit_error
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        if self.enter_error is not None:
            raise self.enter_error
        self.entered = True
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        self.exited = True
        if self.exit_error is not None:
            raise self.exit_error
        return False


class TestCancelAllTasks:
    """Tests for task cancellation."""

    @pytest.mark.asyncio
    async def test_cancel_all_tasks_cancels_pending_tasks(self):
        """Cancel unfinished tasks and wait for their completion."""

        async def wait_forever():
            await asyncio.Future()

        task = asyncio.create_task(wait_forever())
        await helper_functions.cancel_all_tasks({task})

        assert task.done()
        assert task.cancelled()

    @pytest.mark.asyncio
    async def test_cancel_all_tasks_raises_task_exception(self):
        """Propagate exceptions returned by completed tasks."""

        async def fail():
            raise RuntimeError("worker failed")

        task = asyncio.create_task(fail())
        await asyncio.sleep(0)

        with pytest.raises(RuntimeError, match="worker failed"):
            await helper_functions.cancel_all_tasks({task})

    @pytest.mark.asyncio
    async def test_cancel_all_tasks_ignores_cancelled_tasks(self):
        """Ignore asyncio.CancelledError results."""
        task = asyncio.create_task(asyncio.sleep(10))
        task.cancel()

        await helper_functions.cancel_all_tasks({task})

        assert task.cancelled()


class TestIterateSafely:
    """Tests for safe event-bus iteration."""

    @pytest.mark.asyncio
    async def test_iterate_safely_returns_items(self, monkeypatch):
        """Yield items from a successfully registered topic."""

        async def source():
            yield 1
            yield 2

        call = AsyncMock(return_value=source())
        monkeypatch.setattr(helper_functions.event_bus, "call", call)

        result = []
        async for item in helper_functions.iterate_safely("data", "status"):
            result.append(item)

        assert result == [1, 2]
        call.assert_awaited_once_with("data")

    @pytest.mark.asyncio
    async def test_iterate_safely_waits_for_status_after_name_error(self, monkeypatch):
        """Wait for a status update and retry after NameError."""

        async def source():
            yield "ready"

        call = AsyncMock(side_effect=[NameError("not registered"), source()])

        async def subscribe(topic):
            assert topic == "status"
            yield False
            yield True

        monkeypatch.setattr(helper_functions.event_bus, "call", call)
        monkeypatch.setattr(helper_functions.event_bus, "subscribe", subscribe)

        result = [item async for item in helper_functions.iterate_safely("data", "status")]

        assert result == ["ready"]
        assert call.await_count == 2


class TestCallSafely:
    """Tests for safe event-bus calls."""

    @pytest.mark.asyncio
    async def test_call_safely_returns_result(self, monkeypatch):
        """Return the result of a successful event-bus call."""
        call = AsyncMock(return_value="result")
        monkeypatch.setattr(helper_functions.event_bus, "call", call)

        assert await helper_functions.call_safely("data", "status", 1, key="value") == "result"
        call.assert_awaited_once_with("data", 1, key="value")

    @pytest.mark.asyncio
    async def test_call_safely_waits_for_status_after_missing_topic(self, monkeypatch):
        """Wait for registration and retry after TopicNotRegisteredError."""
        call = AsyncMock(side_effect=[TopicNotRegisteredError(), "result"])

        async def subscribe(topic):
            assert topic == "status"
            yield False
            yield True

        monkeypatch.setattr(helper_functions.event_bus, "call", call)
        monkeypatch.setattr(helper_functions.event_bus, "subscribe", subscribe)

        assert await helper_functions.call_safely("data", "status") == "result"
        assert call.await_count == 2


class TestRetry:
    """Tests for stream retry behavior."""

    @pytest.mark.asyncio
    async def test_retry_restarts_source_after_expected_exception(self, monkeypatch):
        """Restart a source after the configured exception."""
        attempts = 0

        async def source():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield "before-error"
                raise ValueError("temporary failure")
            yield "after-error"

        sleep = AsyncMock()
        monkeypatch.setattr(helper_functions.asyncio, "sleep", sleep)

        result = []
        async with helper_functions.streamcontext(
            helper_functions.retry(AsyncIterable(source), ValueError, interval=2)
        ) as streamer:
            async for item in streamer:
                result.append(item)
                if len(result) == 2:
                    break

        assert result == ["before-error", "after-error"]
        assert attempts == 2
        sleep.assert_awaited()

    @pytest.mark.asyncio
    async def test_retry_does_not_catch_unexpected_exception(self):
        """Propagate exceptions other than the configured class."""

        async def source() -> AsyncIterator[None]:
            raise RuntimeError("unexpected")
            yield  # pylint: disable=unreachable

        with pytest.raises(RuntimeError, match="unexpected"):
            async with helper_functions.streamcontext(
                helper_functions.retry(AsyncIterable(source), ValueError)
            ) as streamer:
                async for _ in streamer:
                    pass


class TestContext:
    """Tests for stream context handling."""

    @pytest.mark.asyncio
    async def test_context_calls_enter_and_exit_callbacks(self):
        """Call lifecycle callbacks around a successful stream."""
        context_manager = AsyncContext(value="ctx")
        entered = Mock()
        exited = Mock()

        async def source():
            yield 1
            yield 2

        result = []
        async with helper_functions.streamcontext(
            helper_functions.context(source(), context_manager, entered, exited)
        ) as streamer:
            async for item in streamer:
                result.append(item)

        assert result == [1, 2]
        assert context_manager.entered
        assert context_manager.exited
        entered.assert_called_once_with()
        exited.assert_called_once_with(None)

    @pytest.mark.asyncio
    async def test_context_passes_stream_exception_to_exit_callback(self):
        """Pass a stream exception to the exit callback."""
        context_manager = AsyncContext(value="ctx")
        entered = Mock()
        exited = Mock()

        async def source():
            yield 1
            raise RuntimeError("stream failed")

        with pytest.raises(RuntimeError, match="stream failed"):
            async with helper_functions.streamcontext(
                helper_functions.context(source(), context_manager, entered, exited)
            ) as streamer:
                async for _ in streamer:
                    pass

        entered.assert_called_once_with()
        assert isinstance(exited.call_args.args[0], RuntimeError)
        assert context_manager.exited


class TestWithContext:
    """Tests for the context-yielding operator."""

    @pytest.mark.asyncio
    async def test_with_context_yields_entered_context_and_calls_exit(self):
        """Yield the entered context and call on_exit when cancelled."""
        context_manager = AsyncContext(value="ctx")
        on_exit = Mock()

        async def consume():
            async with helper_functions.streamcontext(
                helper_functions.with_context(context_manager, on_exit)
            ) as streamer:
                async for item in streamer:
                    assert item == "ctx"
                    return

        await consume()
        assert context_manager.entered
        assert context_manager.exited
        on_exit.assert_called_once_with()


class TestFinallyAction:
    """Tests for finally callbacks."""

    @pytest.mark.asyncio
    async def test_finally_action_calls_sync_function(self):
        """Call a synchronous function when the stream closes."""
        callback = Mock()

        async def source():
            yield 1

        result = []
        async with helper_functions.streamcontext(helper_functions.finally_action(source(), callback)) as streamer:
            async for item in streamer:
                result.append(item)

        assert result == [1]
        callback.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_finally_action_awaits_async_function(self):
        """Await an asynchronous function when the stream closes."""
        callback = Mock()

        async def source():
            yield 1

        async with helper_functions.streamcontext(helper_functions.finally_action(source(), callback)) as streamer:
            async for _ in streamer:
                pass

        callback.assert_called_once_with()


class TestCatch:
    """Tests for exception-catching streams."""

    @pytest.mark.asyncio
    async def test_catch_yields_source_items(self):
        """Pass through items when no exception occurs."""

        async def source():
            yield 1
            yield 2

        result = []
        async with helper_functions.streamcontext(helper_functions.catch(source(), ValueError)) as streamer:
            async for item in streamer:
                result.append(item)

        assert result == [1, 2]

    @pytest.mark.asyncio
    async def test_catch_terminates_when_exception_is_caught(self):
        """Terminate normally when the configured exception is raised."""

        async def source():
            yield 1
            raise ValueError("expected")

        result = []
        async with helper_functions.streamcontext(helper_functions.catch(source(), ValueError)) as streamer:
            async for item in streamer:
                result.append(item)

        assert result == [1]

    @pytest.mark.asyncio
    async def test_catch_switches_to_exception_stream(self):
        """Switch to the stream returned by on_exc."""

        async def source() -> AsyncIterator[None]:
            raise ValueError("expected")
            yield  # pylint: disable=unreachable

        def replacement(_exc):
            async def replacement_source():
                yield "replacement"

            return stream.iterate(replacement_source())

        result = []
        async with helper_functions.streamcontext(
            helper_functions.catch(source(), ValueError, replacement)
        ) as streamer:
            async for item in streamer:
                result.append(item)

        assert result == ["replacement"]


class TestCreateDeviceFunction:
    """Tests for device function creation."""

    def test_create_device_function_builds_partial_and_timeout(self):
        """Build a partial with positional and keyword arguments."""
        device = Mock()
        device.measure = Mock(return_value=42)
        config = {
            "function": "measure",
            "args": [1, 2],
            "kwargs": {"unit": "V"},
            "timeout": 3.5,
        }

        function, timeout = helper_functions.create_device_function(device, config)

        assert isinstance(function, partial)
        assert timeout == 3.5
        assert function() == 42
        device.measure.assert_called_once_with(1, 2, unit="V")

    def test_create_device_function_uses_empty_args_and_kwargs(self):
        """Support function calls without optional arguments."""
        device = Mock()
        device.reset = Mock(return_value=True)
        config = {"function": "reset", "timeout": 1.0}

        function, timeout = helper_functions.create_device_function(device, config)

        assert function() is True
        assert timeout == 1.0

    def test_create_device_function_raises_configuration_error_for_missing_function(self):
        """Translate a missing device method into ConfigurationError."""
        device = Mock(spec=[])
        config = {"function": "missing", "timeout": 1.0}

        with pytest.raises(ConfigurationError, match="Function 'missing' not found"):
            helper_functions.create_device_function(device, config)
