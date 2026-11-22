"""Fixtures shared by the tests."""

import pytest

from async_event_bus import event_bus


@pytest.fixture(name="tinkerforge_database")
def tinkerforge_database_fixture():
    """Serve the configs of Tinkerforge bricklets over the event bus, like the database does."""
    configs = {}

    async def get_config(uid):
        return configs.get(uid)

    event_bus.register("db_tinkerforge_sensors/get_config", get_config)
    yield configs
    event_bus.unregister("db_tinkerforge_sensors/get_config")
