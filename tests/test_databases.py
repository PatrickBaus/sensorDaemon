"""Unit tests for the configuration database."""

# The tests intentionally exercise private helpers.
# pylint: disable=protected-access

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pymongo
import pytest
import pytest_asyncio
from beanie import init_beanie
from mongomock_motor import AsyncMongoMockClient

import databases
from async_event_bus import TopicNotRegisteredError, event_bus
from data_types import ChangeType
from database.models import (
    BaseDocument,
    FunctionCall,
    GenericSensorModel,
    LabnodeSensorConfigModel,
    LabnodeSensorModel,
    SensorHostModel,
    TinkerforgeSensorConfigModel,
    TinkerforgeSensorModel,
)


class FakeChangeStream:
    """A change stream that yields the given changes and then raises an error or waits forever."""

    def __init__(self, changes, error=None, end=False):
        self.changes = changes
        self.error = error
        self.end = end
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
        if not self.end:
            await asyncio.Event().wait()  # A change stream does not end, unless it is invalidated


class FakeCollection:  # pylint: disable=too-few-public-methods
    """A collection that serves one fake change stream per call to watch()."""

    def __init__(self, change_streams):
        self.change_streams = list(change_streams)
        self.resume_after = []

    def watch(self, _pipeline, full_document, resume_after):  # pylint: disable=unused-argument
        """Return the next fake change stream and record the resume token used."""
        self.resume_after.append(resume_after)
        return self.change_streams.pop(0)


def fake_model(collection, model_validate=lambda document: document):
    """Create a database model that is backed by a fake collection."""
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


# ----------------------------------------------------------------------------------------------------------------------
# The connection and the contexts that serve the configs. The queries run on an in-memory MongoDB (mongomock-motor).
# ----------------------------------------------------------------------------------------------------------------------

DOCUMENT_MODELS = [BaseDocument, SensorHostModel, TinkerforgeSensorModel, LabnodeSensorModel, GenericSensorModel]


@pytest_asyncio.fixture(name="database")
async def database_fixture():
    """An empty in-memory configuration database."""
    database = AsyncMongoMockClient(uuidRepresentation="standard").sensor_config
    await init_beanie(database=database, document_models=DOCUMENT_MODELS)
    return database


def host_document():
    """A sensor host"""
    return SensorHostModel(
        hostname="192.0.2.1", port=1234, pad=None, driver="generic_ethernet2", reconnect_interval=1, label="Host"
    )


def tinkerforge_document():
    """A Tinkerforge bricklet"""
    config = TinkerforgeSensorConfigModel(interval=1, topic="sensors/t", unit="K")
    return TinkerforgeSensorModel(uid=12345, config={"0": config}, label="Bricklet")


def labnode_document():
    """A LabNode"""
    config = LabnodeSensorConfigModel(interval=1, topic="sensors/l", unit="K", timeout=1)
    return LabnodeSensorModel(uid=1, config={"0": config}, label="LabNode")


def generic_document():
    """A generic sensor of a host"""
    on_read = FunctionCall(function="query_number", args=["MEAS?"], kwargs={}, timeout=1)
    return GenericSensorModel(
        host=uuid4(),
        driver="generic_scpi2",
        interval=1,
        on_read=on_read,
        on_after_read=[],
        topic="sensors/g",
        unit="V",
        label="Sensor",
    )


# The contexts, the model they serve, the key a config is queried with, and the topic of the event bus
CONTEXTS = [
    pytest.param(databases.HostContext, SensorHostModel, host_document, lambda doc: doc.id, id="host"),
    pytest.param(
        databases.TinkerforgeContext,
        TinkerforgeSensorModel,
        tinkerforge_document,
        lambda doc: doc.uid,
        id="tinkerforge",
    ),
    pytest.param(databases.LabnodeContext, LabnodeSensorModel, labnode_document, lambda doc: doc.id, id="labnode"),
    pytest.param(
        databases.GenericSensorContext, GenericSensorModel, generic_document, lambda doc: doc.host, id="generic"
    ),
]


@pytest.mark.parametrize(
    "host,expected",
    [
        ("mongodb://user:secret@db.example:27017/?replicaSet=rs0", "db.example:27017"),
        ("mongodb://user:secret@db.example:27017", "db.example:27017"),
        ("mongodb://db.example:27017", "mongodb://db.example:27017"),
    ],
)
def test_the_hostname_does_not_contain_the_credentials(host, expected):
    """The hostname is used in log messages, so the username, password and options are removed."""
    assert databases.MongoDb(host=host).hostname == expected


@pytest.mark.asyncio
async def test_connecting_retries_until_the_database_is_reachable(monkeypatch, caplog):
    """The connection is retried until the database is reachable. The error is logged once."""
    init = AsyncMock(side_effect=[pymongo.errors.AutoReconnect("down"), pymongo.errors.AutoReconnect("down"), None])
    monkeypatch.setattr(databases, "init_beanie", init)
    monkeypatch.setattr(databases.motor.motor_asyncio, "AsyncIOMotorClient", Mock())
    monkeypatch.setattr(databases.asyncio, "sleep", AsyncMock())

    with caplog.at_level("INFO"):
        async with databases.MongoDb(host="mongodb://user:secret@db.example:27017"):
            pass

    assert init.await_count == 3
    assert caplog.text.count("Cannot connect to config database at db.example:27017") == 1
    assert "Successfully connected to database (db.example:27017)" in caplog.text
    assert "secret" not in caplog.text


@pytest.mark.asyncio
async def test_the_base_context_must_be_implemented():
    """The base class of the contexts only defines the interface."""
    context = databases.Context(topic="test")
    assert context.topic == "test"
    with pytest.raises(NotImplementedError):
        async with context:
            pass
    with pytest.raises(NotImplementedError):
        await context.__aexit__(None, None, None)
    with pytest.raises(NotImplementedError):
        await context.monitor_changes(timeout=1)


@pytest.mark.parametrize(
    "error_code,message",
    [
        (None, "Successfully connected"),
        (-2, "Failure in name resolution"),
        (104, "Database disconnected"),
        (111, "Connection refused"),
        (211, "Initialising replica set"),
        ("resume_token", "there is no token"),
        ("connection reset", "Connection error while monitoring database. Error: connection reset"),
    ],
)
def test_database_errors_are_logged_once(error_code, message, caplog):
    """Each error is logged with a description. A repeated error is not logged again."""
    context = databases.Context(topic="test")
    model = SimpleNamespace(get_settings=lambda: SimpleNamespace(name="SensorHost"))
    with caplog.at_level("INFO"):
        context._log_database_error(model, error_code, previous_error_code=0, timeout=1)
        context._log_database_error(model, error_code, previous_error_code=error_code, timeout=1)
    assert caplog.text.count(message) == 1
    assert "Worker (SensorHost database)" in caplog.text


def test_the_name_of_a_model_without_settings():
    """A model without a collection name is named by its class."""
    model = SimpleNamespace(get_settings=lambda: None)
    assert databases.Context._database_name(model) == str(model)


@pytest.mark.asyncio
@pytest.mark.parametrize("context_class,model,document,key", CONTEXTS)
async def test_the_contexts_serve_the_configs(
    database, context_class, model, document, key
):  # pylint: disable=unused-argument
    """On entering, a context offers the configs on the event bus and announces that it is ready."""
    stored = document()
    await stored.insert()
    context = context_class()
    status = event_bus.subscribe(f"{context.topic}/status_update")
    ready = asyncio.ensure_future(anext(status))
    await asyncio.sleep(0)

    async with context:
        assert await asyncio.wait_for(ready, timeout=1) is True
        config = await event_bus.call(f"{context.topic}/get_config", key(stored))
        assert config["uuid"] == stored.id and "id" not in config
        unknown = -1 if isinstance(key(stored), int) else uuid4()
        assert await event_bus.call(f"{context.topic}/get_config", unknown) is None

    with pytest.raises(TopicNotRegisteredError):
        await event_bus.call(f"{context.topic}/get_config", key(stored))
    await status.aclose()


@pytest.mark.asyncio
async def test_the_host_context_lists_all_hosts(database):  # pylint: disable=unused-argument
    """The host context lists the ids of all hosts, so that they can be started."""
    hosts = [host_document(), host_document()]
    hosts[1].port = 4321  # (hostname, port) is unique
    for host in hosts:
        await host.insert()

    async with databases.HostContext():
        found = [uuid async for uuid in await event_bus.call("db_autodiscovery_sensors/get")]

    assert sorted(found) == sorted(host.id for host in hosts)


@pytest.mark.asyncio
@pytest.mark.parametrize("context_class,model,document,key", CONTEXTS)
@pytest.mark.parametrize(
    "errors,returns_config,log",
    [
        ([ValueError("invalid document")], False, "ERROR"),
        ([pymongo.errors.AutoReconnect("down"), pymongo.errors.AutoReconnect("down")], True, "INFO"),
        ([pymongo.errors.OperationFailure("no keys", code=211)] * 2, True, "INFO"),
        ([pymongo.errors.OperationFailure("unauthorized", code=13)], False, "ERROR"),
        ([pymongo.errors.PyMongoError("broken")], False, "ERROR"),
    ],
    ids=["invalid", "reconnect", "replica set starting", "failure", "error"],
)
async def test_errors_while_querying_a_config(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    database, monkeypatch, caplog, context_class, model, document, key, errors, returns_config, log
):  # pylint: disable=unused-argument
    """
    A config that cannot be read is None. While the database is not available, the query is repeated. Errors are
    logged.
    """
    stored = document()
    monkeypatch.setattr(model, "find_one", AsyncMock(side_effect=[*errors, stored]))
    monkeypatch.setattr(databases.asyncio, "sleep", AsyncMock())

    with caplog.at_level("INFO"):
        async with context_class() as context:
            config = await event_bus.call(f"{context.topic}/get_config", key(stored))

    assert (config is not None) == returns_config
    assert caplog.records and caplog.records[-1].levelname == log


@pytest.mark.asyncio
@pytest.mark.parametrize("context_class,model,document,key", CONTEXTS)
async def test_the_contexts_publish_the_changes(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    database, monkeypatch, context_class, model, document, key
):  # pylint: disable=unused-argument
    """The changes of the database are published on the event bus, so that the sensors can be reconfigured."""
    changed = document()
    removed_uuid = uuid4()
    context = context_class()
    expected = {
        databases.HostContext: [
            (f"nodes/by_uuid/{changed.id}/update", "config"),
            ("db_autodiscovery_sensors/add_host", changed.id),
            (f"nodes/by_uuid/{removed_uuid}/update", None),
        ],
        databases.LabnodeContext: [
            (f"nodes/by_uuid/{changed.id}/update", "config"),
            (f"nodes/by_uuid/{changed.id}/add", "config"),
            (f"nodes/by_uuid/{removed_uuid}/update", None),
        ],
        databases.GenericSensorContext: [
            (f"nodes/by_uuid/{changed.id}/update", "config"),
            (f"nodes/by_uuid/{getattr(changed, 'host', None)}/add", "config"),
            (f"nodes/by_uuid/{removed_uuid}/update", None),
        ],
        databases.TinkerforgeContext: [
            (f"nodes/by_uuid/{changed.id}/remove", None),
            (f"nodes/tinkerforge/{getattr(changed, 'uid', None)}/update", "config"),
            (f"nodes/by_uuid/{changed.id}/remove", None),
            (f"nodes/tinkerforge/{getattr(changed, 'uid', None)}/update", "config"),
            (f"nodes/by_uuid/{removed_uuid}/remove", None),
        ],
    }[context_class]
    published = []
    monkeypatch.setattr(databases.event_bus, "publish", lambda topic, event: published.append((topic, event)))

    async def changes(*_args):
        yield ChangeType.UPDATE, changed
        yield ChangeType.ADD, changed
        yield ChangeType.REMOVE, removed_uuid

    monkeypatch.setattr(context, "_monitor_database", changes)
    await context.monitor_changes(timeout=1)

    assert [(topic, "config" if isinstance(event, dict) else event) for topic, event in published] == expected
    configs = [event for _, event in published if isinstance(event, dict)]
    assert all(config["uuid"] == changed.id and "id" not in config for config in configs)


@pytest.mark.asyncio
async def test_monitor_database_converts_the_changes():
    """Updates, replacements and inserts yield the new document, deletions the uuid. Other events are skipped."""
    removed = uuid4()
    collection = FakeCollection(
        [
            FakeChangeStream(
                [
                    {"_id": "token-1", "operationType": "update", "fullDocument": "updated"},
                    {"_id": "token-2", "operationType": "replace", "fullDocument": "replaced"},
                    {"_id": "token-3", "operationType": "insert", "fullDocument": "inserted"},
                    {"_id": "token-4", "operationType": "drop"},  # not a change of a document
                    {"_id": "token-5", "operationType": "delete", "documentKey": {"_id": removed}},
                ]
            )
        ]
    )
    context = databases.Context(topic="test")

    changes = await asyncio.wait_for(take(context._monitor_database(fake_model(collection), timeout=0), 4), 1)

    assert changes == [
        (ChangeType.UPDATE, "updated"),
        (ChangeType.UPDATE, "replaced"),
        (ChangeType.ADD, "inserted"),
        (ChangeType.REMOVE, removed),
    ]


@pytest.mark.asyncio
async def test_monitor_database_continues_after_the_end_of_a_change_stream():
    """If a change stream ends, e.g. because it was invalidated, a new one is opened where the old one ended."""
    collection = FakeCollection(
        [FakeChangeStream([delete_change("token-1")], end=True), FakeChangeStream([delete_change("token-2")])]
    )
    context = databases.Context(topic="test")

    await asyncio.wait_for(take(context._monitor_database(fake_model(collection), timeout=0), 2), 1)

    assert collection.resume_after == [None, "token-1"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,message",
    [
        (pymongo.errors.AutoReconnect("[Errno 111] Connection refused"), "Connection refused. Waiting to start."),
        (pymongo.errors.OperationFailure("no keys", code=211), "Initialising replica set"),
        (pymongo.errors.OperationFailure("unauthorized", code=13), "Error: unauthorized"),
        (pymongo.errors.PyMongoError("cursor failed"), "there is no token"),
    ],
)
async def test_monitor_database_reconnects_after_errors(error, message, caplog):
    """Errors of the change stream are logged and the change stream is opened again."""
    collection = FakeCollection([FakeChangeStream([], error=error), FakeChangeStream([delete_change("token-1")])])
    context = databases.Context(topic="test")

    with caplog.at_level("ERROR"):
        await asyncio.wait_for(take(context._monitor_database(fake_model(collection), timeout=0), 1), 1)

    assert message in caplog.text
    assert collection.resume_after == [None, None]


@pytest.mark.asyncio
async def test_monitor_database_drops_the_resume_token_if_resuming_fails(caplog):
    """If the change stream fails after changes were processed, it starts from scratch and warns about lost changes."""
    collection = FakeCollection(
        [
            FakeChangeStream([delete_change("token-1")], error=pymongo.errors.PyMongoError("resume failed")),
            FakeChangeStream([delete_change("token-2")]),
        ]
    )
    context = databases.Context(topic="test")

    with caplog.at_level("ERROR"):
        await asyncio.wait_for(take(context._monitor_database(fake_model(collection), timeout=0), 2), 1)

    assert collection.resume_after == [None, None]
    assert "Changes made while disconnected may have been missed" in caplog.text
