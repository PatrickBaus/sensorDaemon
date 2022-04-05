# -*- coding: utf-8 -*-
"""
This file contains an ip connection class, that simplifies the asyncio streamreader and streamwriter
"""
import asyncio
import errno
import logging


class ConnectionLostError(ConnectionError):
    """
    Raised if the connection is terminated during a read or write.
    """


class NotConnectedError(ConnectionError):
    """
    Raised whenever the connection is not connected and there is a read or write attempt.
    """


class IpConnection:
    SEPARATOR = "\n"   # The default message separator (will be encoded to bytes later)

    @property
    def hostname(self):
        """
        Returns
        -------
        str
            The hostname of the sensor host.
        """
        return self.__hostname

    @property
    def port(self):
        """
        Returns
        -------
        int
            The port of connection.
        """
        return self.__port

    @property
    def connection_timeout(self):
        """
        Returns
        -------
        int
            The port of connection.
        """
        return self.__timeout

    @property
    def is_connected(self):
        return self.__writer is not None and not self.__writer.is_closing()

    def __init__(self, hostname, port, timeout=None):
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
        self.__writer, self.__reader = None, None
        self.__logger = logging.getLogger(__name__)
        self.__read_lock = asyncio.Lock()

    def __str__(self):
        return f"ethernet connection ({self.hostname}:{self.port})"

    async def __aenter__(self):
        await self.connect()

        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.disconnect()

    async def connect(self, hostname=None, port=None, timeout=None):
        if self.is_connected:
            return

        self.__hostname = hostname if hostname is not None else self.__hostname
        self.__port = port if port is not None else self.__port
        self.__timeout = timeout if timeout is not None else self.__timeout

        # wait_for() blocks until the request is done if timeout is None
        self.__reader, self.__writer = await asyncio.wait_for(
            asyncio.open_connection(self.__hostname, self.__port),
            timeout=self.__timeout
        )

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

    async def __read(self, length=None, separator=None, timeout=None):
        if not self.is_connected:
            raise NotConnectedError("Not connected")

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
            # Strip the separator if we are using one
            data = data[:-len(separator)] if length is None else data

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

    async def read(self, length=None, separator=None, timeout=None):
        async with self.__read_lock:
            return await self.__read(length, separator, timeout)

    async def write(self, cmd, timeout=None):
        if not self.is_connected:
            raise NotConnectedError("Not connected")

        self.__writer.write(cmd)
        # wait_for() blocks until the request is done if timeout is None
        await asyncio.wait_for(self.__writer.drain(), timeout=timeout)

    async def query(self, cmd, length=None, separator=None, timeout=None):
        if not self.is_connected:
            raise NotConnectedError("Not connected")

        async with self.__read_lock:
            await self.write(cmd, timeout)
            result = await self.__read(length, separator, timeout)
            return result
