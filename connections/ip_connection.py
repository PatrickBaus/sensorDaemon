"""
This file contains an ip connection class, that simplifies the asyncio streamreader and streamwriter
"""

from __future__ import annotations

import asyncio
import errno
import logging
from asyncio import StreamReader, StreamWriter
from types import TracebackType
from typing import Type

try:
    from typing import Self  # type: ignore # Python 3.11
except ImportError:
    from typing_extensions import Self


class ConnectionLostError(ConnectionError):
    """
    Raised if the connection is terminated during a read or write.
    """


class NotConnectedError(ConnectionError):
    """
    Raised when the connection is not connected and there is a read or write attempt.
    """


class HostUnreachableError(ConnectionError):
    """
    Raised when there is no route to the host
    """


class HostRefusedError(ConnectionError):
    """
    Raised when the host refuses the connection
    """


class GenericIpConnection:
    """
    A generic ip connection, that wraps the asyncio stream reader/writer and creates a context manager for connecting
    and disconnecting the ip connection.
    """

    @property
    def hostname(self) -> str:
        """
        Returns
        -------
        str
            The hostname of the sensor host.
        """
        return self.__hostname

    @property
    def port(self) -> int:
        """
        Returns
        -------
        int
            The port of connection.
        """
        return self.__port

    @property
    def uri(self) -> str:
        """
        Returns
        -------
        str
            A string representation of the connection.
        """
        return f"{self.hostname}:{self.port}"

    @property
    def timeout(self) -> float | None:
        """
        Returns
        -------
        float
            The timeout in seconds
        """
        return self.__timeout

    def get_connection_timeout(self) -> float | None:
        """
        An alias of the timeout property

        Returns
        -------
        float
            The timeout in seconds
        """
        return self.timeout

    @property
    def is_connected(self) -> bool:
        """
        Returns
        -------
        bool
            True if the connection is up.
        """
        return self.__writer is not None and not self.__writer.is_closing()

    def __init__(self, hostname: str, port: int, timeout: float | None = None) -> None:
        """
        Create new IpConnection. `hostname` and `port` parameters can be None if they are provided when calling
        `connect()`

        Parameters
        ----------
        hostname: str
            IP or hostname of the machine hosting the sensor daemon
        port: int
            port on the host
        timeout int or None
            The timeout used during initial connection. The read timeout can be given separately to the `read()` call.
        """
        self.__hostname, self.__port = hostname, port
        self.__timeout = timeout
        self.__writer: StreamWriter | None
        self.__reader: StreamReader | None
        self.__writer, self.__reader = None, None
        self.__logger = logging.getLogger(__name__)
        self.__read_lock = asyncio.Lock()

    def __str__(self) -> str:
        return f"GenericIpConnection({self.hostname}:{self.port})"

    async def __aenter__(self) -> Self:
        await self.connect()

        return self

    async def __aexit__(
        self, exc_type: Type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        await self.disconnect()

    async def connect(self, hostname: str | None = None, port: int | None = None, timeout: float | None = None) -> None:
        """
        Connect to the host. It is better to use the context manager, which ensures that the ip connection is properly
        disconnected.

        Parameters
        ----------
        hostname: str
            The hostname of the remote host
        port: int
            The port of the remote host
        timeout: float, optional
            The timeout in seconds. If the value is not passed, use the value given to the constructor.
        """
        if self.is_connected:
            return

        self.__hostname = hostname if hostname is not None else self.__hostname
        self.__port = port if port is not None else self.__port
        self.__timeout = timeout if timeout is not None else self.__timeout

        try:
            # wait_for() blocks until the request is done if timeout is None
            self.__reader, self.__writer = await asyncio.wait_for(
                asyncio.open_connection(self.__hostname, self.__port), timeout=self.__timeout
            )
        except OSError as exc:
            if exc.errno in (errno.ENETUNREACH, errno.EHOSTUNREACH):
                raise HostUnreachableError(f"Host unreachable ({self.hostname}:{self.port})") from None
            if exc.errno == errno.ECONNREFUSED:
                raise HostRefusedError(f"Host refused the connection ({self.hostname}:{self.port})") from None
            raise

    async def disconnect(self):
        """
        Disconnect the ip connection and flush the caches.
        """
        if not self.is_connected:
            return
        # Flush data
        try:
            self.__writer.write_eof()
            await self.__writer.drain()
            self.__writer.close()
            await self.__writer.wait_closed()
        except ConnectionError:
            # Ignore connection related errors, because we are dropping the connection anyway
            pass
        except OSError as exc:
            if exc.errno == errno.ENOTCONN:
                pass  # Socket is no longer connected, so we can't send the EOF.
            else:
                raise
        finally:
            self.__writer, self.__reader = None, None

    async def __read(
        self, length: int | None = None, separator: bytes | None = None, timeout: float | None = None
    ) -> bytes:
        if not self.is_connected:
            raise NotConnectedError("Not connected")
        assert self.__reader is not None

        if length is None:
            coro = self.__reader.readuntil(separator if separator is not None else b"\n")
        else:
            if length > 0:
                coro = self.__reader.readexactly(length)
            else:
                coro = self.__reader.read(length)

        try:
            # wait_for() blocks until the request is done if timeout is None
            data = await asyncio.wait_for(coro, timeout=timeout)
            self.__logger.debug("Data read from host (%s:%d): %s", data, self.__hostname, self.__port)

            return data
        except asyncio.TimeoutError:
            self.__logger.debug("Timeout (>%g s) while reading data from %s", timeout, self)
            raise
        except asyncio.IncompleteReadError as exc:
            if len(exc.partial) > 0:  # pylint: disable=no-else-return
                self.__logger.warning(
                    "Incomplete read request from host (%s:%d). Check your data.", self.__hostname, self.__port
                )
                return exc.partial
            else:
                self.__logger.error("Connection error. The host (%s:%d) did not reply.", self.__hostname, self.__port)
                try:
                    await self.disconnect()
                except Exception:  # pylint: disable=broad-except
                    # We could get back *anything*. So we catch everything and throw it away.
                    # We are shutting down anyway.
                    self.__logger.exception("Exception during read error.")
                raise ConnectionLostError(
                    f"IP connection error. The host '{self.__hostname}:{self.__port}' did not reply"
                ) from None
        except asyncio.LimitOverrunError:  # pylint: disable=try-except-raise
            # TODO: catch asyncio.LimitOverrunError?
            raise

    async def read(
        self, length: int | None = None, character: bytes | None = None, timeout: float | None = None
    ) -> bytes:
        """
        Read from the ip connection.
        Parameters
        ----------
        length: int, optional
            If given, read only `length` number of bytes.
        character: bytes, optional
            Read until the separator. The separator will be returned.
        timeout: float, optional
            The timeout in seconds. If the value is not passed, use the value given to the constructor.

        Returns
        -------
        bytes:
            The string read from the ip connection

        Raises
        ------
        NotConnectedError
            If the ip connection is not connected.
        """
        async with self.__read_lock:
            return await self.__read(length=length, separator=character, timeout=timeout)

    async def write(self, cmd: bytes, timeout: float | None = None) -> None:
        """
        Write to the ip connection
        Parameters
        ----------
        cmd: bytes
            The string to be written
        timeout: float or None
            The number of seconds to wait when draining the write-cache. Use None to wait indefinitely

        Raises
        ------
        NotConnectedError
            If the ip connection is not connected.
        """
        if not self.is_connected:
            raise NotConnectedError("Not connected")
        assert self.__writer is not None

        self.__writer.write(cmd)
        # wait_for() blocks until the request is done if timeout is None
        await asyncio.wait_for(self.__writer.drain(), timeout=timeout)

    async def query(
        self, cmd: bytes, length: int | None = None, separator: bytes | None = None, timeout: float | None = None
    ) -> bytes:
        """
        Write to the connection and read back the result.
        Parameters
        ----------
        cmd: bytes
            The command to write
        length: int, optional
            If given, read only `length` number of bytes.
        separator: bytes, optional
            Read until the separator. The separator will be returned.
        timeout: float, optional
            The timeout in seconds. If the value is not passed, use the value given to the constructor for reading and
            an indefinite timeout for writing.

        Returns
        -------
        bytes
            The string read from the ip connection
        """
        if not self.is_connected:
            raise NotConnectedError("Not connected")

        async with self.__read_lock:
            await self.write(cmd, timeout)
            result = await self.__read(length, separator, timeout)
            return result
