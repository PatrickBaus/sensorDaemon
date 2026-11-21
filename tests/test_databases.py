"""Unit tests for the database change monitoring."""

# The tests intentionally exercise private helpers.
# pylint: disable=protected-access

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pymongo
import pytest

import databases
from data_types import ChangeType
from database.models import TinkerforgeSensorConfigModel


class FakeChangeStream:
    """A change stream, that yields the given changes and then raises an error or waits forever."""

    def __init__(self, changes, error=None):
        self.changes = changes
        self.error = error
        self.resume_token = None

    async def __aenter__(self):
        """Enter the fake change stream context."""
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        """Leave the fake change stream context."""
        return None

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for change in self.changes:
            self.resume_token = change["_id"]
            yield change
        if self.error is not None:
            raise self.error
        await asyncio.Event().wait()  # A change stream never ends on its own


class FakeCollection:  # pylint: disable=too-few-public-methods
    """A collection, that serves one fake change stream per call to watch()."""

    def __init__(self, change_streams):
        self.change_streams = list(change_streams)
        self.resume_after = []

    def watch(self, _pipeline, full_document, resume_after):  # pylint: disable=unused-argument
        """Return the next fake change stream and record the resume token used."""
        self.resume_after.append(resume_after)
        return self.change_streams.pop(0)


def fake_model(collection, model_validate=lambda document: document):
    """Create a database model, that is backed by a fake collection."""
    return SimpleNamespace(
        get_motor_collection=lambda: collection,
        get_settings=lambda: SimpleNamespace(name="Test"),
        model_validate=model_validate,
    )


def delete_change(token):
    """Create a change stream event for a deleted document."""
    return {"_id": token, "operationType": "delete", "documentKey": {"_id": uuid4()}}


async def take(generator, count):
    """Read `count` items from an async generator, then close it."""
    items = []
    try:
        async for item in generator:
            items.append(item)
            if len(items) == count:
                break
    finally:
        await generator.aclose()
    return items


@pytest.mark.asyncio
async def test_monitor_database_resumes_after_connection_loss():
    """After a connection loss, the change stream continues from the last processed change."""
    collection = FakeCollection(
        [
            FakeChangeStream([delete_change("token-1")], error=pymongo.errors.AutoReconnect("connection lost")),
            FakeChangeStream([delete_change("token-2")]),
        ]
    )
    context = databases.Context(topic="test")

    changes = await asyncio.wait_for(take(context._monitor_database(fake_model(collection), timeout=0), 2), 1)

    assert [change_type for change_type, _ in changes] == [ChangeType.REMOVE, ChangeType.REMOVE]
    assert collection.resume_after == [None, "token-1"]


@pytest.mark.asyncio
async def test_monitor_database_drops_a_resume_token_that_is_no_longer_valid(caplog):
    """If the change stream cannot be resumed from the token, start from scratch instead of retrying forever."""
    collection = FakeCollection(
        [
            FakeChangeStream([delete_change("token-1")], error=pymongo.errors.AutoReconnect("connection lost")),
            FakeChangeStream([], error=pymongo.errors.OperationFailure("history lost", code=286)),
            FakeChangeStream([delete_change("token-2")]),
        ]
    )
    context = databases.Context(topic="test")

    with caplog.at_level("ERROR"):
        await asyncio.wait_for(take(context._monitor_database(fake_model(collection), timeout=0), 2), 1)

    assert collection.resume_after == [None, "token-1", None]
    assert "Changes made while disconnected may have been missed" in caplog.text


@pytest.mark.asyncio
async def test_monitor_database_skips_invalid_documents(caplog):
    """An invalid document is logged and skipped. Monitoring continues with the next change."""
    invalid = {"_id": "token-1", "operationType": "insert", "fullDocument": {"topic": "missing interval"}}
    valid = {
        "_id": "token-2",
        "operationType": "insert",
        "fullDocument": {"interval": 1000, "topic": "sensors/test", "unit": "K"},
    }
    collection = FakeCollection([FakeChangeStream([invalid, valid])])
    context = databases.Context(topic="test")
    model = fake_model(collection, model_validate=TinkerforgeSensorConfigModel.model_validate)

    with caplog.at_level("ERROR"):
        changes = await asyncio.wait_for(take(context._monitor_database(model, timeout=0), 1), 1)

    change_type, document = changes[0]
    assert change_type is ChangeType.ADD
    assert document.topic == "sensors/test"
    assert collection.resume_after == [None]  # The stream was not restarted
    assert "Ignoring invalid document" in caplog.text
