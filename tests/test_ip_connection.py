"""Unit tests for the generic IP connection."""

import asyncio

import pytest

from connections.ip_connection import GenericIpConnection


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
