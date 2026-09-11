"""Unit tests for the generic driver mixin."""

# Test fixtures intentionally exercise protected implementation details and share
# fixture names with test parameters. Helper classes also intentionally have few
# public methods, so these Pylint checks are not useful for this test module.
# pylint: disable=protected-access,redefined-outer-name,too-few-public-methods

import asyncio
import inspect
from functools import partial
from unittest.mock import AsyncMock, Mock, call

import pytest

from errors import ConfigurationError
from sensors.drivers import generic_driver
from sensors.drivers.generic_driver import GenericDriverMixin


class DriverUnderTest(GenericDriverMixin):
    """Test DriverUnderTest behavior."""

    def __init__(self, uuid="test-uuid"):
        """Provide a test helper."""
        super().__init__(uuid)

    def __str__(self):
        """Provide a test helper."""
        return "Test driver"


class Pipeable:
    """Minimal stand-in for an aiostream Stream used by construction tests."""

    def __or__(self, other):
        """Provide a test helper."""
        return self


class FakeStream:
    """Test FakeStream behavior."""

    def __init__(self):
        """Provide a test helper."""
        self.calls = []

    def empty(self):
        """Provide a test helper."""
        self.calls.append(("empty", (), {}))
        return Pipeable()

    def iterate(self, *args, **kwargs):
        """Provide a test helper."""
        self.calls.append(("iterate", args, kwargs))
        return Pipeable()

    def repeat(self, *args, **kwargs):
        """Provide a test helper."""
        self.calls.append(("repeat", args, kwargs))
        return Pipeable()

    def just(self, *args, **kwargs):
        """Provide a test helper."""
        self.calls.append(("just", args, kwargs))
        return Pipeable()

    def chain(self, *args, **kwargs):
        """Provide a test helper."""
        self.calls.append(("chain", args, kwargs))
        return Pipeable()


class FakePipe:
    """Test FakePipe behavior."""

    def __init__(self):
        """Provide a test helper."""
        self.calls = []

    def __getattr__(self, name):
        """Provide a test helper."""

        def operator(*args, **kwargs):
            """Provide a test helper."""
            self.calls.append((name, args, kwargs))
            return Pipeable()

        return operator


@pytest.fixture
def driver():
    """Provide a fixture for the tests."""
    return DriverUnderTest()


class TestGenericDriverMixin:
    """Test TestGenericDriverMixin behavior."""

    def test_init_stores_uuid_and_calls_super(self, driver):
        """Test init stores uuid and calls super."""
        assert driver._GenericDriverMixin__uuid == "test-uuid"

    def test_parse_config_none_returns_none(self, driver):
        """Test parse config none returns none."""
        assert driver._parse_config(None) is None

    def test_parse_config_converts_function_calls(self, driver, monkeypatch):
        """Test parse config converts function calls."""
        calls = []

        def create_device_function(device, func_call):
            """Provide a test helper."""
            calls.append((device, func_call))
            return partial(lambda *args, **kwargs: None), func_call["timeout"]

        monkeypatch.setattr(generic_driver, "create_device_function", create_device_function)
        config = {
            "on_connect": [{"function": "connect", "timeout": 1}],
            "on_disconnect": [{"function": "disconnect", "timeout": 2}],
            "on_read": {"function": "read", "timeout": 3},
            "on_after_read": [{"function": "after", "timeout": 4}],
        }

        result = driver._parse_config(config)

        assert result is config
        assert len(calls) == 4
        assert isinstance(config["on_connect"], tuple)
        assert isinstance(config["on_disconnect"], tuple)
        assert isinstance(config["on_after_read"], tuple)
        assert isinstance(config["on_read"], tuple)

    def test_parse_config_returns_none_on_configuration_error(self, driver, monkeypatch, caplog):
        """Test parse config returns none on configuration error."""

        def create_device_function(*args):
            """Provide a test helper."""
            raise ConfigurationError("bad function")

        monkeypatch.setattr(generic_driver, "create_device_function", create_device_function)

        config = {
            "on_connect": [],
            "on_disconnect": [],
            "on_read": {"function": "missing", "timeout": 1},
            "on_after_read": [],
        }

        with caplog.at_level("ERROR"):
            assert driver._parse_config(config) is None

        assert "Invalid configuration" in caplog.text

    @pytest.mark.asyncio
    async def test_clean_up_calls_all_functions_with_individual_timeouts(self, driver):
        """Test clean up calls all functions with individual timeouts."""
        first = AsyncMock()
        second = AsyncMock()

        await driver._clean_up([(first, 1.0), (second, 2.0)])

        first.assert_awaited_once()
        second.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_clean_up_logs_exceptions_and_continues(self, driver, caplog):
        """Test clean up logs exceptions and continues."""
        failing = AsyncMock(side_effect=RuntimeError("shutdown failed"))
        succeeding = AsyncMock()

        with caplog.at_level("ERROR"):
            await driver._clean_up([(failing, 1.0), (succeeding, 1.0)])

        succeeding.assert_awaited_once()
        assert "Error during shutdown" in caplog.text

    @pytest.mark.asyncio
    async def test_clean_up_applies_timeout(self, driver):
        """Test clean up applies timeout."""

        async def never_finishes():
            """Provide a test helper."""
            await asyncio.sleep(10)

        wait_for = Mock()

        async def fake_wait_for(awaitable, timeout):
            """Provide a test helper."""
            wait_for(awaitable, timeout)
            awaitable.close()
            raise asyncio.TimeoutError

        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(generic_driver.asyncio, "wait_for", fake_wait_for)
        try:
            await driver._clean_up([(never_finishes, 0.25)])
        finally:
            monkeypatch.undo()

        wait_for.assert_called_once()
        assert wait_for.call_args.args[1] == 0.25

    def test_on_error_returns_empty_stream(self, driver, monkeypatch):
        """Test on error returns empty stream."""
        empty = Mock(return_value="empty-stream")
        monkeypatch.setattr(generic_driver.stream, "empty", empty)

        result = driver.on_error(ValueError("read failed"))

        assert result == "empty-stream"
        empty.assert_called_once_with()

    def test_configure_and_stream_none_returns_empty_stream(self, driver, monkeypatch):
        """Test configure and stream none returns empty stream."""
        empty = Mock(return_value="empty-stream")
        monkeypatch.setattr(generic_driver.stream, "empty", empty)

        assert driver._configure_and_stream(None) == "empty-stream"
        empty.assert_called_once_with()

    def test_log_config_progress_none(self, driver, caplog):
        """Test log config progress none."""
        with caplog.at_level("INFO"):
            driver._log_config_progress(None)
        assert "Invalid configuration" in caplog.text

    @pytest.mark.parametrize("enabled", [True, False])
    def test_log_config_progress_enabled_state(self, driver, caplog, enabled):
        """Test log config progress enabled state."""
        config = {"enabled": enabled}
        with caplog.at_level("INFO"):
            driver._log_config_progress(config)

        expected = "Enabling device" if enabled else "Disabling device"
        assert expected in caplog.text

    def test_read_device_async_generator_callback(self, driver, monkeypatch):
        """Test read device async generator callback."""
        stream_mock = FakeStream()
        pipe_mock = FakePipe()
        monkeypatch.setattr(generic_driver, "stream", stream_mock)
        monkeypatch.setattr(generic_driver, "pipe", pipe_mock)

        async def read_generator():
            """Provide a test helper."""
            yield 42

        on_read = partial(read_generator)
        config = {"on_read": (on_read, 0.75), "interval": 10}

        result = driver._read_device(config)

        assert isinstance(result, Pipeable)
        assert stream_mock.calls[0][0] == "iterate"
        assert inspect.isasyncgen(stream_mock.calls[0][1][0])
        assert ("map",) == tuple(x[:1] for x in pipe_mock.calls)[0]
        assert ("timeout", (0.75,), {}) in pipe_mock.calls

    def test_read_device_regular_callback(self, driver, monkeypatch):
        """Test read device regular callback."""
        stream_mock = FakeStream()
        pipe_mock = FakePipe()
        monkeypatch.setattr(generic_driver, "stream", stream_mock)
        monkeypatch.setattr(generic_driver, "pipe", pipe_mock)

        on_read = partial(lambda: [1, 2, 3])
        config = {"on_read": (on_read, 0.5), "interval": 2.0}

        result = driver._read_device(config)

        assert isinstance(result, Pipeable)
        assert stream_mock.calls[0] == ("repeat", ((on_read, 0.5),), {"interval": 2.0})
        operator_names = [name for name, _, _ in pipe_mock.calls]
        assert operator_names == ["starmap", "concat"]

        # The per-read timeout is created inside the starmap callback, not while
        # constructing the outer stream. Execute that callback to verify it.
        starmap_callback = pipe_mock.calls[0][1][0]
        pipe_mock.calls.clear()
        starmap_callback(on_read, 0.5)
        assert [name for name, _, _ in pipe_mock.calls] == [
            "concatmap",
            "enumerate",
            "timeout",
        ]
        assert pipe_mock.calls[-1][1] == (0.5,)

    def test_stream_data_builds_stream_for_initial_config(self, driver, monkeypatch):
        """Test stream data builds stream for initial config."""
        stream_mock = FakeStream()
        pipe_mock = FakePipe()
        monkeypatch.setattr(generic_driver, "stream", stream_mock)
        monkeypatch.setattr(generic_driver, "pipe", pipe_mock)

        subscribe = Mock(return_value=iter(()))
        monkeypatch.setattr(generic_driver.event_bus, "subscribe", subscribe)

        result = driver.stream_data({"enabled": False})

        assert isinstance(result, Pipeable)
        assert subscribe.call_args == call("nodes/by_uuid/test-uuid/update")
        assert any(name == "chain" for name, _, _ in stream_mock.calls)
        assert any(name == "switchmap" for name, _, _ in pipe_mock.calls)

    def test_stream_data_uses_uuid_in_subscription_topic(self, monkeypatch):
        """Test stream data uses uuid in subscription topic."""
        driver = DriverUnderTest(uuid="abc-123")
        stream_mock = FakeStream()
        pipe_mock = FakePipe()
        monkeypatch.setattr(generic_driver, "stream", stream_mock)
        monkeypatch.setattr(generic_driver, "pipe", pipe_mock)
        subscribe = Mock(return_value=iter(()))
        monkeypatch.setattr(generic_driver.event_bus, "subscribe", subscribe)

        driver.stream_data({"enabled": False})

        subscribe.assert_called_once_with("nodes/by_uuid/abc-123/update")


class TestConfigureAndStreamBehavior:
    """Test TestConfigureAndStreamBehavior behavior."""

    def test_parse_config_preserves_original_function_call_arguments(self, driver, monkeypatch):
        """Test parse config preserves original function call arguments."""
        created = []

        def factory(_device, func_call):
            """Provide a test helper."""
            created.append(func_call)
            return partial(lambda: None), 1.0

        monkeypatch.setattr(generic_driver, "create_device_function", factory)
        config = {
            "on_connect": [{"function": "a", "args": [1], "kwargs": {"x": 2}, "timeout": 3}],
            "on_disconnect": [],
            "on_read": {"function": "b", "timeout": 4},
            "on_after_read": [],
        }

        driver._parse_config(config)

        assert created[0]["args"] == [1]
        assert created[0]["kwargs"] == {"x": 2}
