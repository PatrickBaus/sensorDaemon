"""
This is an asyncIO driver for a generic SCPI compatible device.
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from aiostream import async_, pipe, stream

from sensors.drivers.generic_driver import GenericDriver

if TYPE_CHECKING:
    from sensors.transports.ethernet import EthernetTransport
    from sensors.transports.prologix_ethernet import PrologixEthernetTransport


class ScpiIoError(ValueError):
    pass


class GenericScpiDriver:
    TERMINATOR = "\n"  # The default message terminator (will be encoded to bytes later)

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
            raise ValueError(f"Device returned invalid ID: {result!r}")
        result: tuple[str, str, str, str]
        return result

    async def read(self, scpi_terminator: str | None = None, *args, **kwargs) -> str:
        """
        Read a single value from the device. If `length' is given, read `length` bytes, else
        read until a line break.

        Parameters
        ----------
        scpi_terminator: str, default=None
            One or more characters, that terminate SCPI replies. If not set, the default terminator '\n' will be used.
            The terminator will be stripped from the end of the data.

        Returns
        ----------
        Decimal or bytes
            Either a value or a number of bytes as defined by `length`.
        """
        # use the default terminator if none is given
        try:
            scpi_terminator = self.TERMINATOR.encode() if scpi_terminator is None else scpi_terminator.encode()
        except UnicodeEncodeError:
            self.__logger.warning("Invalid terminator '%r', using default: '%r'", scpi_terminator, self.TERMINATOR)
            scpi_terminator = self.TERMINATOR.encode()

        data = await self._conn.read(*args, **kwargs)
        # Strip the terminator if we are using one and the last bytes match the terminator
        if scpi_terminator and data[-len(scpi_terminator):] == scpi_terminator:
            data = data[:-len(scpi_terminator)]
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            # drop it. It is not SCPI compliant
            self.__logger.error("Invalid data read '%r'. This driver requires ASCII or UTF-8 data", data)
            raise ScpiIoError("Received invalid data from device %s", self)

    async def write(self, cmd: str, scpi_terminator: str | None = None) -> None:
        if scpi_terminator is None:
            cmd += self.TERMINATOR
        else:
            cmd += scpi_terminator
        try:
            await self._conn.write(cmd.encode())
        except UnicodeEncodeError:
            self.__logger.error("Cannot write invalid command '%r'. Use ASCII or UTF-8 strings only.", cmd)
            raise ScpiIoError("Cannot write illegal command %s to device %s", self)

    async def query(self, cmd: str, scpi_terminator: str | None = None, *args, **kwargs) -> str:
        await self.write(cmd, scpi_terminator)
        return await self.read(scpi_terminator, *args, **kwargs)

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

    async def query_number(self, cmd: str, length: int | None = None, scpi_terminator: str | None = None) -> Decimal:
        await self.write(cmd, scpi_terminator)
        return await self.read_number(length, scpi_terminator)


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
        maximum_tries = 2
        manufacturer = None
        while maximum_tries:
            try:
                manufacturer, model_number, serial_number, revision = await self.get_id()
                self.device_name = f"{manufacturer} {model_number} ({serial_number})"
            except ValueError:
                continue  # silently retry it once more
            else:
                break  # Stop the loop if everything went well
            finally:
                maximum_tries -= 1

        # Send a warning, if we did not get a valid id
        if manufacturer is None:
            logging.getLogger(__name__).warning("Could not query '*IDN?' of device: %s", self)

    def stream_data(self, config):
        return (
            stream.chain(
                stream.just(self)
                    | pipe.action(async_(lambda sensor: sensor.enumerate()))
                    | pipe.filter(lambda x: False)
                , super().stream_data(config)
            )
        )
