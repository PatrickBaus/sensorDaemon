"""Tests for the daemon: its configuration from the environment and its lifecycle."""

import asyncio
import logging
import os
import signal
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from decouple import UndefinedValueError

import kraken_ethernet
from kraken_ethernet import Kraken, load_secret, parse_log_level


@pytest.fixture(name="environment")
def environment_fixture(monkeypatch):
    """The environment variables, that Kraken needs to start."""
    for name in ("NODE_ID", "MQTT_CLIENT_USER", "MQTT_CLIENT_PASSWORD", "SENSORS_DATABASE_USERNAME"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(f"{name}_FILE", raising=False)
    monkeypatch.setenv("MQTT_HOST", "broker.example")
    monkeypatch.setenv("SENSORS_DATABASE_HOST", "mongodb://db.example")
    return monkeypatch


def test_a_secret_is_read_from_the_environment(environment):
    """Without a secret file, the secret is read from the environment variable."""
    environment.setenv("MY_SECRET", "from the environment")
    assert load_secret("MY_SECRET") == "from the environment"


def test_a_secret_file_is_preferred(environment, tmp_path):
    """A Docker secret file (MY_SECRET_FILE) is preferred over the environment variable."""
    secret_file = tmp_path / "secret"
    secret_file.write_text("from the file\n", encoding="utf-8")
    environment.setenv("MY_SECRET", "from the environment")
    environment.setenv("MY_SECRET_FILE", str(secret_file))
    assert load_secret("MY_SECRET") == "from the file"


def test_a_missing_secret(environment):
    """A missing secret is replaced by the default. Without a default, it is an error."""
    environment.delenv("MY_SECRET", raising=False)
    assert load_secret("MY_SECRET", default=None) is None
    with pytest.raises(UndefinedValueError, match="MY_SECRET not found"):
        load_secret("MY_SECRET")


def test_the_parameters_of_the_database_and_the_broker(environment, tmp_path):
    """The credentials are optional. They can be given as environment variables or secret files."""
    assert Kraken.load_database_parameters() == {"host": "mongodb://db.example"}

    password_file = tmp_path / "password"
    password_file.write_text("secret", encoding="utf-8")
    environment.setenv("SENSORS_DATABASE_USERNAME", "kraken")
    environment.setenv("SENSORS_DATABASE_PASSWORD_FILE", str(password_file))
    environment.setenv("MQTT_CLIENT_USER", "kraken")
    assert Kraken.load_database_parameters() == {
        "host": "mongodb://db.example",
        "username": "kraken",
        "password": "secret",
    }
    mqtt = Kraken.load_mqtt_parameters()
    assert (mqtt.hosts, mqtt.username, mqtt.password) == ([("broker.example", 1883)], "kraken", None)


@pytest.mark.parametrize(
    "value,level",
    [
        (10, logging.DEBUG),
        ("20", logging.INFO),
        ("warning", logging.WARNING),
        ("ERROR", logging.ERROR),
        ("x", logging.INFO),
    ],
)
def test_parse_log_level(value, level):
    """The log level is given as a number or a name. Unknown names select INFO."""
    assert parse_log_level(value) == level


class FakeManager:  # pylint: disable=too-few-public-methods
    """Replaces a manager. Its run() is given by the test."""

    behaviour = None

    def __init__(self, *_args, **_kwargs):
        pass

    async def run(self):
        """Run the behaviour of the test, or run until cancelled."""
        if self.behaviour is None:
            await asyncio.Event().wait()
        await self.behaviour()  # pylint: disable=not-callable


def fake_managers(monkeypatch, host_manager_behaviour):
    """Replace the managers of Kraken. The host manager runs `host_manager_behaviour`, the others run until stopped."""

    class FakeHostManager(FakeManager):  # pylint: disable=too-few-public-methods
        """The host manager"""

        behaviour = staticmethod(host_manager_behaviour)

    monkeypatch.setattr(kraken_ethernet, "MqttManager", FakeManager)
    monkeypatch.setattr(kraken_ethernet, "DatabaseManager", FakeManager)
    monkeypatch.setattr(kraken_ethernet, "HostManager", FakeHostManager)


@pytest.mark.asyncio
async def test_kraken_stops_on_sigterm(environment, caplog):
    """A SIGTERM, e.g. from docker stop, stops all workers and Kraken exits with 0."""

    async def terminate():
        os.kill(os.getpid(), signal.SIGTERM)  # The signal handler was installed, before the workers were started
        await asyncio.Event().wait()

    fake_managers(environment, terminate)
    with caplog.at_level("WARNING"):
        assert await asyncio.wait_for(Kraken().run(), timeout=5) == 0

    assert "Received shutdown request" in caplog.text
    assert "No node is set" in caplog.text


async def fail():
    """A worker, that fails"""
    raise RuntimeError("broken")


async def end():
    """A worker, that stops on its own"""


async def cancel():
    """A worker, that is cancelled"""
    asyncio.current_task().cancel()
    await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "behaviour,message",
    [
        (fail, "Worker host manager stopped with an error."),
        (end, "Worker host manager stopped unexpectedly."),
        (cancel, "Worker host manager was cancelled unexpectedly."),
    ],
)
async def test_kraken_exits_with_an_error_if_a_worker_stops(environment, caplog, behaviour, message):
    """A worker must never stop. If one does, the reason is logged and Kraken exits with 1, so that it is restarted."""
    environment.setenv("NODE_ID", "12345678-1234-5678-1234-567812345678")
    fake_managers(environment, behaviour)

    with caplog.at_level("WARNING"):
        assert await asyncio.wait_for(Kraken().run(), timeout=5) == 1

    assert message in caplog.text
    assert f"This is the node with id: {UUID('12345678-1234-5678-1234-567812345678')}" in caplog.text


@pytest.mark.asyncio
async def test_errors_while_stopping_the_workers_are_logged(caplog):
    """If a worker fails while it is stopped, the error is logged."""

    async def fails_when_cancelled():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise RuntimeError("cleanup failed") from None

    worker = asyncio.create_task(fails_when_cancelled())
    await asyncio.sleep(0)
    with caplog.at_level("ERROR"):
        await Kraken().shutdown()

    assert worker.done()
    assert "Error while reaping tasks during shutdown" in caplog.text


@pytest.mark.asyncio
async def test_main_returns_the_exit_code(monkeypatch):
    """main() returns the exit code of Kraken. A cancelled Kraken exits with 0."""
    monkeypatch.setattr(Kraken, "run", AsyncMock(return_value=1))
    assert await kraken_ethernet.main() == 1

    monkeypatch.setattr(Kraken, "run", AsyncMock(side_effect=asyncio.CancelledError))
    assert await kraken_ethernet.main() == 0
