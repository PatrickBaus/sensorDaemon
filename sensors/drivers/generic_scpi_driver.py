"""
This is an asyncIO driver for a generic SCPI compatible device.
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, AsyncGenerator

from aiostream import async_, pipe, stream

from data_types import DataEvent
from sensors.drivers.generic_driver import GenericDriverMixin

if TYPE_CHECKING:
    from sensors.transports.ethernet import EthernetTransport
    from sensors.transports.linux_gpib import LinuxGpibTransport
    from sensors.transports.prologix_ethernet import PrologixEthernetTransport


class ScpiIoError(ValueError):
    """Raised if either invalid data is sent by device or if invalid data is requested to be written to the device"""


class GenericScpiMixin:
    """A mixin to add basic SCPI commands and capabilities to a driver."""

    TERMINATOR = "\n"  # The default message terminator (will be encoded to bytes later)

    @property
    def device_name(self) -> str:
        """
        Returns
        -------
        str
            The device name as defined by the manufacturer, obtained through the *IDN? command.
        """
        return "Generic SCPI device" if self.__device_name is None else self.__device_name

    @device_name.setter
    def device_name(self, name: str):
        self.__device_name: str | None = name

    def __init__(
        self, connection: EthernetTransport | LinuxGpibTransport | PrologixEthernetTransport, *args: Any, **kwargs: Any
    ) -> None:
        self._conn = connection
        self.__device_name = None

        self.__logger = logging.getLogger(__name__)
        # Call the base class constructor, because this is just a mixin, that comes before the base class in the MRO,
        # so there *might* be a base class.
        super().__init__(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.device_name} at {str(self._conn)}"

    async def wait_for_opc(self, timeout: float | None = None) -> None:
        """
        Wait for the device to signal completion of all currently queued commands.

        Parameters
        ----------
        timeout: float or None
            Read timeout in seconds. Use None for an infinite timeout.
        """
        await self.write("*OPC?")
        while (await asyncio.wait_for(self.read(), timeout=timeout)) != "1":
            await asyncio.sleep(0.1)

    async def get_id(self) -> tuple[str, str, str, str]:
        """
        Returns a tuple with 4 elements, that contain the manufacturer name, model number, serial number and revision
        :return:
        """
        idn: str = await self.query("*IDN?")
        try:
            company_name, model, serial, firmware = idn.split(",")
        except ValueError:
            raise ValueError(f"Device returned invalid ID: {idn!r}") from None
        return company_name, model, serial, firmware

    async def read(self, *args: Any, scpi_terminator: str | None = None, **kwargs: Any) -> str:
        """
        Read a single value from the device. If `length' is given, read `length` bytes, else
        read until a line break.

        Parameters
        ----------
        scpi_terminator: str, optional
            One or more characters, that terminate SCPI replies. If not set, the default terminator '\n' will be used.
            The terminator will be stripped from the end of the data.
        *args: Any
            The arguments passed on to the connection object.
        **kwargs: Any
            The keyword arguments passed on to the connection object.

        Returns
        ----------
        str
            The data from the device
        """
        # use the default terminator if none is given
        try:
            terminator = self.TERMINATOR.encode() if scpi_terminator is None else scpi_terminator.encode()
        except UnicodeEncodeError:
            self.__logger.warning("Invalid terminator '%r', using default: '%r'", scpi_terminator, self.TERMINATOR)
            terminator = self.TERMINATOR.encode()

        data = await self._conn.read(*args, **kwargs)
        # Strip the terminator if we are using one and the last bytes match the terminator
        if data[-len(terminator) :] == terminator:
            data = data[: -len(terminator)]
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            # drop it. It is not SCPI compliant
            self.__logger.error("Invalid data read '%r'. This driver requires ASCII or UTF-8 data", data)
            raise ScpiIoError(f"Received invalid data from device {self}") from None

    async def write(self, cmd: str, scpi_terminator: str | None = None) -> None:
        """
        Write a command to the device.

        Parameters
        ----------
        cmd: str
            The command to be written.
        scpi_terminator: str, optional
            The line terminator of the SCPI device. Omit to use the default "\n".
        """
        if scpi_terminator is None:
            cmd += self.TERMINATOR
        else:
            cmd += scpi_terminator
        try:
            await self._conn.write(cmd.encode())
        except UnicodeEncodeError:
            self.__logger.error("Cannot write invalid command '%r'. Use ASCII or UTF-8 strings only.", cmd)
            raise ScpiIoError(
                f"Cannot write illegal command %s to device {self}",
            ) from None

    async def query(self, cmd: str, *args: Any, scpi_terminator: str | None = None, **kwargs: Any) -> str:
        """
        Send a command and read back the response.

        Parameters
        ----------
        cmd: str
            The command to be sent.
        scpi_terminator: str, optional
            The line terminator used for sending and receiving. Omit to use the default "\n".
        *args: Any
            The arguments passed on to the connection object.
        **kwargs: Any
            The keyword arguments passed on to the connection object.

        Returns
        -------
        str
            The result of the query
        """
        await self.write(cmd, scpi_terminator)
        return await self.read(scpi_terminator, *args, **kwargs)

    async def read_number(self, *args: Any, scpi_terminator: str | None = None, **kwargs: Any) -> Decimal:
        """
        Read a number from the device.

        Parameters
        ----------
        scpi_terminator: str, optional
            The line terminator. Omit to use the default "\n".
        *args: Any
            The arguments passed on to the connection object.
        **kwargs: Any
            The keyword arguments passed on to the connection object.

        Returns
        -------
        Decimal
            The number read from the device. This might also be NaN, Infinity, or -Infinity.
        """
        result = Decimal(await self.read(scpi_terminator, *args, **kwargs))
        # Treat special SCPI values
        # Not A Number
        if result == Decimal("9.91e37"):
            return Decimal("NaN")
        # Positive infinity
        if result == Decimal("9.9e37"):
            return Decimal("Infinity")
        # Negative infinity
        if result == Decimal("-9.9e37"):
            return Decimal("-Infinity")
        return result

    async def query_number(self, cmd: str, scpi_terminator: str | None = None) -> Decimal:
        """
        Send a command and read back a number.

        Parameters
        ----------
        cmd: str
            The command to be sent
        scpi_terminator: str, optional
            The line terminator. Omit to use the default "\n".

        Returns
        -------
        Decimal
            The number read from the device. This might also be NaN, Infinity, or -Infinity.
        """
        await self.write(cmd, scpi_terminator)
        return await self.read_number(scpi_terminator)


class GenericScpiDriver(GenericDriverMixin, GenericScpiMixin):
    """This is a basic generic SCPI driver, that implements the streaming interface and SCPI functionality."""

    @classmethod
    def driver(cls) -> str:
        """
        Returns
        -------
        str
            The driver name that identifies it to the sensor factory
        """
        return "generic_scpi2"

    def __init__(
        self,
        uuid,
        connection: EthernetTransport | LinuxGpibTransport | PrologixEthernetTransport,
        *_args: Any,
        **_kwargs: Any,
    ) -> None:
        super().__init__(uuid, connection)

    async def enumerate(self) -> None:
        """Query the device for its ID using the *IDN? command"""
        maximum_tries = 2
        manufacturer = None
        while maximum_tries:
            try:
                (
                    manufacturer,
                    model_number,
                    serial_number,
                    revision,  # pylint: disable=unused-variable  # For documentation purposes
                ) = await self.get_id()
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

    def stream_data(self, config: dict[str, Any]) -> AsyncGenerator[DataEvent, None]:
        """
        Enumerate the device, then read data from it.

        Parameters
        ----------
        config: dict
            A dict containing the configuration for the device

        Yields
        -------
        DataEvent
            The data from the device
        """
        return stream.chain(
            stream.just(self) | pipe.action(async_(lambda sensor: sensor.enumerate())) | pipe.filter(lambda x: False),
            super().stream_data(config),
        )
