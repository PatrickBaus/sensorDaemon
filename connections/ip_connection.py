"""
This file contains an ip connection class, that simplifies the asyncio streamreader and streamwriter
"""
from __future__ import annotations

import asyncio
import errno
import logging
from asyncio import StreamReader, StreamWriter
from types import TracebackType
from typing import Optional, Type

try:
    from typing import Self  # Python 3.11
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
    def timeout(self) -> float:
        """
        Returns
        -------
        float
            The timeout in seconds
        """
        return self.__timeout

    def get_connection_timeout(self):
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
        return self.__writer is not None and not self.__writer.is_closing()

    def __init__(self, hostname: str, port: int, timeout: Optional[int] = None) -> None:
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
            The timeout used during initial connection. The read timeout must be given separately to the `read()` call.
        """
        self.__hostname, self.__port = hostname, port
        self.__timeout = timeout
        self.__writer: StreamWriter | None
        self.__reader: StreamReader | None
        self.__writer, self.__reader = None, None
        self.__logger = logging.getLogger(__name__)
        self.__read_lock = asyncio.Lock()

    def __str__(self) -> str:
        return f"Ethernet connection ({self.hostname}:{self.port})"

    async def __aenter__(self) -> Self:
        await self.connect()

        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc: Optional[BaseException],
            traceback: Optional[TracebackType]
    ) -> None:
        await self.disconnect()

    async def connect(self, hostname: Optional[str] = None, port: Optional[int] = None, timeout: Optional[float] = None) -> None:
        if self.is_connected:
            return

        self.__hostname = hostname if hostname is not None else self.__hostname
        self.__port = port if port is not None else self.__port
        self.__timeout = timeout if timeout is not None else self.__timeout

        try:
            # wait_for() blocks until the request is done if timeout is None
            self.__reader, self.__writer = await asyncio.wait_for(
                asyncio.open_connection(self.__hostname, self.__port),
                timeout=self.__timeout
            )
        except OSError as exc:
            if exc.errno in (errno.ENETUNREACH, errno.EHOSTUNREACH):
                raise HostUnreachableError(f"Host unreachable ({self.hostname}:{self.port})") from None
            elif exc.errno == errno.ECONNREFUSED:
                raise HostRefusedError(f"Host refused the connection ({self.hostname}:{self.port})") from None
            else:
                raise

    async def disconnect(self):
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
                pass    # Socket is no longer connected, so we can't send the EOF.
            else:
                raise
        finally:
            self.__writer, self.__reader = None, None

    async def __read(self, length: Optional[int] = None, separator: Optional[bytes] = None, timeout: Optional[float] = None) -> bytes:
        if not self.is_connected:
            raise NotConnectedError("Not connected")
        assert length is not None or separator is not None, "Either specify the number of bytes to read or a separator"

        if length is None:
            coro = self.__reader.readuntil(separator)
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
                    f"IP connection error. The host '{self.__hostname}:{self.__port}' did not reply") from None
        except asyncio.LimitOverrunError:
            # TODO: catch asyncio.LimitOverrunError?
            raise

    async def read(self, length: Optional[int] = None, character: Optional[bytes] = None, timeout: Optional[int] = None) -> bytes:
        async with self.__read_lock:
            return await self.__read(length=length, separator=character, timeout=timeout)

    async def write(self, cmd: bytes, timeout: Optional[float] = None) -> None:
        if not self.is_connected:
            raise NotConnectedError("Not connected")

        self.__writer.write(cmd)
        # wait_for() blocks until the request is done if timeout is None
        await asyncio.wait_for(self.__writer.drain(), timeout=timeout)

    async def query(self, cmd: bytes, length: Optional[int] = None, separator: Optional[bytes] = None, timeout: Optional[float] = None) -> bytes:
        if not self.is_connected:
            raise NotConnectedError("Not connected")

        async with self.__read_lock:
            await self.write(cmd, timeout)
            result = await self.__read(length, separator, timeout)
            return result
