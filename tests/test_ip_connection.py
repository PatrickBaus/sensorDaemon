"""Unit tests for the generic IP connection."""

import asyncio
import errno
from unittest.mock import AsyncMock, Mock

import pytest

from connections.ip_connection import (
    ConnectionLostError,
    GenericIpConnection,
    HostRefusedError,
    HostUnreachableError,
    NotConnectedError,
)


@pytest.mark.asyncio
async def test_read_logs_the_data_at_debug_level(caplog):
    """A read at debug log level logs the host, port and data without a logging error."""

    async def handle(_reader, writer):
        writer.write(b"1.234\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        with caplog.at_level("DEBUG", logger="connections.ip_connection"):
            async with GenericIpConnection("127.0.0.1", port) as connection:
                assert await connection.read(timeout=1) == b"1.234\n"
    finally:
        server.close()
        await server.wait_closed()

    assert f"Data read from host (127.0.0.1:{port}): b'1.234\\n'" in caplog.text


class FakeHost:
    """A host on a local TCP port, that runs `handler` for each connection."""

    def __init__(self, handler) -> None:
        self.handler = handler
        self.server: asyncio.Server | None = None
        self.port = 0

    async def __aenter__(self) -> "FakeHost":
        self.server = await asyncio.start_server(self.handler, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *_exc_info) -> None:
        assert self.server is not None
        self.server.close()
        await self.server.wait_closed()


async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Send back every line received."""
    while line := await reader.readline():
        writer.write(line)
        await writer.drain()
    writer.close()


def reply_once(data: bytes):
    """A handler, that sends `data` and closes the connection."""

    async def handler(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(data)
        await writer.drain()
        writer.close()

    return handler


@pytest.mark.asyncio
async def test_properties():
    """The connection describes its host."""
    connection = GenericIpConnection("example.com", 1234, timeout=2.5)
    assert (connection.hostname, connection.port, connection.uri) == ("example.com", 1234, "example.com:1234")
    assert connection.timeout == connection.get_connection_timeout() == 2.5
    assert str(connection) == "GenericIpConnection(example.com:1234)"
    assert not connection.is_connected


@pytest.mark.asyncio
async def test_write_and_query():
    """Data is written and the reply is read up to the line terminator."""
    async with FakeHost(echo) as host:
        async with GenericIpConnection("127.0.0.1", host.port) as connection:
            assert connection.is_connected
            await connection.write(b"first\n")
            assert await connection.read(timeout=1) == b"first\n"
            assert await connection.query(b"second\n", timeout=1) == b"second\n"
            assert await connection.query(b"a;b;\n", separator=b";", timeout=1) == b"a;"
        assert not connection.is_connected


@pytest.mark.asyncio
async def test_connect_overrides_and_reconnects():
    """connect() can override the host, port and timeout. Connecting a connected connection does nothing."""
    async with FakeHost(echo) as host:
        connection = GenericIpConnection("unused.example", 1)
        await connection.connect("127.0.0.1", host.port, timeout=1)
        await connection.connect("other.example", 2)  # already connected, nothing changes
        assert (connection.hostname, connection.port, connection.timeout) == ("127.0.0.1", host.port, 1)
        await connection.disconnect()
        await connection.disconnect()  # disconnecting twice is fine


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "length,expected", [(3, b"123"), (-1, b"1234567890")], ids=["exactly n bytes", "until the end"]
)
async def test_read_a_number_of_bytes(length, expected):
    """A positive length reads exactly that many bytes, a negative length reads until the host closes."""
    async with FakeHost(reply_once(b"1234567890")) as host:
        async with GenericIpConnection("127.0.0.1", host.port) as connection:
            assert await connection.read(length=length, timeout=1) == expected


@pytest.mark.asyncio
async def test_incomplete_data_is_returned_with_a_warning(caplog):
    """If the host closes the connection in the middle of a message, the partial message is returned."""
    async with FakeHost(reply_once(b"partial")) as host:
        async with GenericIpConnection("127.0.0.1", host.port) as connection:
            with caplog.at_level("WARNING"):
                assert await connection.read(timeout=1) == b"partial"
    assert "Incomplete read request" in caplog.text


@pytest.mark.asyncio
async def test_a_closed_connection_is_reported_as_lost(caplog):
    """If the host closes the connection without sending anything, the connection is dropped."""
    async with FakeHost(reply_once(b"")) as host:
        async with GenericIpConnection("127.0.0.1", host.port) as connection:
            with pytest.raises(ConnectionLostError):
                await connection.read(timeout=1)
            assert not connection.is_connected
    assert "did not reply" in caplog.text


@pytest.mark.asyncio
async def test_errors_while_dropping_a_lost_connection_are_logged(monkeypatch, caplog):
    """An error while disconnecting a lost connection is logged. The connection is reported as lost anyway."""
    async with FakeHost(reply_once(b"")) as host:
        connection = GenericIpConnection("127.0.0.1", host.port)
        await connection.connect()
        monkeypatch.setattr(connection, "disconnect", AsyncMock(side_effect=RuntimeError("broken")))
        with pytest.raises(ConnectionLostError):
            await connection.read(timeout=1)
    assert "Exception during read error" in caplog.text


@pytest.mark.asyncio
async def test_read_timeout():
    """A read, that does not finish within the timeout, raises a TimeoutError."""
    async with FakeHost(echo) as host:
        async with GenericIpConnection("127.0.0.1", host.port) as connection:
            with pytest.raises(TimeoutError):
                await connection.read(timeout=0.01)


@pytest.mark.asyncio
async def test_a_message_longer_than_the_buffer_raises_an_error():
    """More than 64 KiB without a line terminator exceed the buffer of the stream reader."""
    async with FakeHost(reply_once(b"x" * 2**17)) as host:
        async with GenericIpConnection("127.0.0.1", host.port) as connection:
            with pytest.raises(asyncio.LimitOverrunError):
                await connection.read(timeout=1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call",
    [
        lambda connection: connection.read(),
        lambda connection: connection.write(b"data"),
        lambda connection: connection.query(b"data"),
    ],
    ids=["read", "write", "query"],
)
async def test_requests_need_a_connection(call):
    """Reading or writing without a connection raises a NotConnectedError."""
    with pytest.raises(NotConnectedError):
        await call(GenericIpConnection("127.0.0.1", 1))


@pytest.mark.asyncio
async def test_connection_refused():
    """A host, that refuses the connection, raises a HostRefusedError."""
    async with FakeHost(echo) as host:
        port = host.port
    # The host is gone, so the port is closed
    with pytest.raises(HostRefusedError):
        await GenericIpConnection("127.0.0.1", port).connect()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,expected",
    [
        (OSError(errno.EHOSTUNREACH, "No route to host"), HostUnreachableError),
        (OSError(errno.ENETUNREACH, "Network is unreachable"), HostUnreachableError),
        # OSError(EACCES) creates a PermissionError
        (OSError(errno.EACCES, "Permission denied"), PermissionError),
    ],
)
async def test_connection_errors(monkeypatch, error, expected):
    """Unreachable hosts raise a HostUnreachableError. Other errors are passed on."""
    monkeypatch.setattr(asyncio, "open_connection", AsyncMock(side_effect=error))
    with pytest.raises(expected) as exc_info:
        await GenericIpConnection("192.0.2.1", 1234).connect()
    assert exc_info.type is expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [ConnectionResetError(), OSError(errno.ENOTCONN, "not connected")], ids=["reset", "not connected"]
)
async def test_errors_while_disconnecting_are_ignored(error):
    """A connection, that is already broken, is dropped without an error."""
    async with FakeHost(echo) as host:
        connection = GenericIpConnection("127.0.0.1", host.port)
        await connection.connect()
        writer = connection._GenericIpConnection__writer  # pylint: disable=protected-access
        writer.write_eof = Mock(side_effect=error)
        await connection.disconnect()
        writer.close()
    assert not connection.is_connected


@pytest.mark.asyncio
async def test_other_errors_while_disconnecting_are_raised():
    """Unexpected errors while disconnecting are passed on, but the connection is dropped anyway."""
    async with FakeHost(echo) as host:
        connection = GenericIpConnection("127.0.0.1", host.port)
        await connection.connect()
        writer = connection._GenericIpConnection__writer  # pylint: disable=protected-access
        writer.write_eof = Mock(side_effect=OSError(errno.EACCES, "Permission denied"))
        with pytest.raises(OSError):
            await connection.disconnect()
        writer.close()
    assert not connection.is_connected
