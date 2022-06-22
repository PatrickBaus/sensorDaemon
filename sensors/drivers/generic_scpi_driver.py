"""
This is an asyncIO driver for a generic SCPI compatible device.
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

from aiostream import async_, pipe, stream

from sensors.drivers.generic_driver import GenericDriver

if TYPE_CHECKING:
    from sensors.transports.ethernet import EthernetTransport
    from sensors.transports.prologix_ethernet import PrologixEthernetTransport


class ScpiIoError(ValueError):
    pass


class GenericScpiDriver:
    SEPARATOR = "\n"  # The default message separator (will be encoded to bytes later)

    @property
    def device_name(self) -> str:
        return "Generic SCPI device" if self.__device_name is None else self.__device_name

    @device_name.setter
    def device_name(self, name: str):
        self.__device_name = name

    def __init__(
            self,
            connection: EthernetTransport | PrologixEthernetTransport,
    ) -> None:
        self._conn = connection
        self.__device_name = None

        self.__logger = logging.getLogger(__name__)

    def __str__(self) -> str:
        return f"{self.device_name} at {str(self._conn)}"

    async def wait_for_opc(self, timeout: float | None = None) -> None:
        await self.write("*OPC?")
        while (await asyncio.wait_for(self.read(), timeout=timeout)) != '1':
            await asyncio.sleep(0.1)

    async def get_id(self) -> tuple[str, str, str, str]:
        """
        Returns a tuple with 4 elements, that contain the manufacturer name, model number, serial number and revision
        :return:
        """
        result: str = await self.query("*IDN?")
        result: tuple[str, ...] = tuple(result.split(","))
        if len(result) != 4:
            raise ValueError("Device returned invalid ID: %r", result)
        result: tuple[str, str, str, str]
        return result

    async def read(self, length: int | None = None, separator: str | None = None) -> str:
        """
        Read a single value from the device. If `length' is given, read `length` bytes, else
        read until a line break.

        Parameters
        ----------
        length: int, default=None
            The number of bytes to read. Omit to read a line.

        separator: str, default=None
            One or more characters, that separate replies. If not set, the default
            separator '\n' will be used if length is not set.

        Returns
        ----------
        Decimal or bytes
            Either a value or a number of bytes as defined by `length`.
        """
        if length is None:
            # use the default separator if none is given
            try:
                separator = self.SEPARATOR.encode() if separator is None else separator.encode()
            except UnicodeEncodeError:
                self.__logger.warning("Invalid separator '%r', using default: '%r'", separator, self.SEPARATOR)
                separator = self.SEPARATOR.encode()

        data = await self._conn.read(length=length, character=separator)
        # Strip the separator if we are using one
        data = data[:-len(separator)] if length is None else data
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            # drop it. It is not SCPI compliant
            self.__logger.error("Invalid data read '%r'. This driver requires ASCII or UTF-8 data", data)
            raise ScpiIoError("Received invalid data from device %s", self)

    async def write(self, cmd: str, separator: str | None = None) -> None:
        if separator is None:
            cmd += self.SEPARATOR
        else:
            cmd += separator
        try:
            await self._conn.write(cmd.encode())
        except UnicodeEncodeError:
            self.__logger.error("Cannot write invalid command '%r'. Use ASCII or UTF-8 strings only.", cmd)
            raise ScpiIoError("Cannot write illegal command %s to device %s", self)

    async def query(self, cmd: str, length: int | None = None, separator: str | None = None) -> str:
        await self.write(cmd, separator)
        return await self.read(length, separator)

    async def read_number(self, length: int | None = None, separator: str | None = None) -> Decimal:
        result = await self.read(length, separator)
        # Treat special SCPI values
        # Not A Number
        if result.lower() == "9.91e37":
            return Decimal('NaN')
        # Positive infinity
        elif result.lower() == "9.9e37":
            return Decimal('Infinity')
        # Negative infinity
        elif result.lower() == "-9.9e37":
            return Decimal('-Infinity')
        return Decimal(result)

    async def query_number(self, cmd: str, length: int | None = None, separator: str | None = None) -> Decimal:
        await self.write(cmd, separator)
        return await self.read_number(length, separator)


class GenericScpiSensor(GenericDriver, GenericScpiDriver):
    """This class extends the SCPI driver with catch-all arguments in the constructor"""
    @classmethod
    @property
    def driver(cls) -> str:
        """
        Returns
        -------
        str
            The driver that identifies it to the host factory
        """
        return "generic_scpi2"

    def __init__(
            self,
            uuid,
            connection: EthernetTransport | PrologixEthernetTransport,
            *_args,
            **_kwargs
    ) -> None:
        super().__init__(uuid, connection)

    async def enumerate(self):
        manufacturer, model_number, serial_number, revision = await self.get_id()
        self.device_name = f"{manufacturer} {model_number} ({serial_number})"

    def stream_data(self, config):
        return (
            stream.chain(
                stream.just(self)
                    | pipe.action(async_(lambda sensor: sensor.enumerate()))
                    | pipe.filter(lambda x: False)
                , super().stream_data(config)
            )
        )
