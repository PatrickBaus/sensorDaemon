"""Unit tests for the generic SCPI driver."""

# Test fixtures intentionally exercise protected implementation details and share
# fixture names with test parameters. The large mixin test class is also intentional.
# pylint: disable=protected-access,redefined-outer-name,too-many-public-methods,unused-argument

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from sensors.drivers.generic_scpi_driver import GenericScpiDriver, GenericScpiMixin, ScpiIoError


class FakeConnection:
    """Test FakeConnection behavior."""

    def __init__(self, read_data=b""):
        """Provide a test helper."""
        self.read_data = read_data
        self.writes = []

    async def read(self, *args, **kwargs):
        """Provide a test helper."""
        return self.read_data

    async def write(self, data):
        """Provide a test helper."""
        self.writes.append(data)

    def __str__(self):
        """Provide a test helper."""
        return "fake-connection"


@pytest.fixture
def connection():
    """Provide a fixture for the tests."""
    return FakeConnection()


@pytest.fixture
def mixin(connection):
    """Provide a fixture for the tests."""
    return GenericScpiMixin(connection)


@pytest.fixture
def driver(connection):
    """Provide a fixture for the tests."""
    return GenericScpiDriver("test-uuid", connection)


class TestGenericScpiMixin:
    """Test TestGenericScpiMixin behavior."""

    def test_default_device_name(self, mixin):
        """Test default device name."""
        assert mixin.device_name == "Generic SCPI device"

    def test_device_name_can_be_set(self, mixin):
        """Test device name can be set."""
        mixin.device_name = "Test Instrument"
        assert mixin.device_name == "Test Instrument"

    def test_str(self, mixin):
        """Test str."""
        mixin.device_name = "Test Instrument"
        assert str(mixin) == "Test Instrument at fake-connection"

    @pytest.mark.asyncio
    async def test_write_adds_default_terminator(self, mixin, connection):
        """Test write adds default terminator."""
        await mixin.write("MEAS:VOLT?")
        assert connection.writes == [b"MEAS:VOLT?\n"]

    @pytest.mark.asyncio
    async def test_write_uses_custom_terminator(self, mixin, connection):
        """Test write uses custom terminator."""
        await mixin.write("MEAS:VOLT?", scpi_terminator="\r\n")
        assert connection.writes == [b"MEAS:VOLT?\r\n"]

    @pytest.mark.asyncio
    async def test_write_allows_empty_terminator(self, mixin, connection):
        """Test write allows empty terminator."""
        await mixin.write("TEST", scpi_terminator="")
        assert connection.writes == [b"TEST"]

    @pytest.mark.asyncio
    async def test_write_invalid_unicode_raises_scpi_io_error(self, mixin):
        """Test write invalid unicode raises scpi io error."""
        with pytest.raises(ScpiIoError, match="Cannot write illegal command"):
            await mixin.write("TEST\ud800")

    @pytest.mark.asyncio
    async def test_read_strips_default_terminator(self, mixin, connection):
        """Test read strips default terminator."""
        connection.read_data = b"123.45\n"
        assert await mixin.read() == ["123.45"]

    @pytest.mark.asyncio
    async def test_read_strips_custom_terminator(self, mixin, connection):
        """Test read strips custom terminator."""
        connection.read_data = b"123.45\r\n"
        assert await mixin.read(scpi_terminator="\r\n") == ["123.45"]

    @pytest.mark.asyncio
    async def test_read_with_empty_terminator_preserves_data(self, mixin, connection):
        """Test read with empty terminator preserves data."""
        connection.read_data = b"1,2,3"
        assert await mixin.read(scpi_terminator="") == ["1", "2", "3"]

    @pytest.mark.asyncio
    async def test_read_splits_comma_separated_values(self, mixin, connection):
        """Test read splits comma separated values."""
        connection.read_data = b"1,2,3\n"
        assert await mixin.read() == ["1", "2", "3"]

    @pytest.mark.asyncio
    async def test_read_rejects_invalid_utf8(self, mixin, connection):
        """Test read rejects invalid utf8."""
        connection.read_data = b"\xff\xfe\n"
        with pytest.raises(ScpiIoError, match="Received invalid data"):
            await mixin.read()

    @pytest.mark.asyncio
    async def test_read_invalid_terminator_falls_back_to_default(self, mixin, connection, caplog):
        """Test read invalid terminator falls back to default."""
        connection.read_data = b"123\n"
        with caplog.at_level("WARNING"):
            result = await mixin.read(scpi_terminator="\ud800")
        assert result == ["123"]
        assert "Invalid terminator" in caplog.text

    @pytest.mark.asyncio
    async def test_query_writes_then_reads(self, mixin, connection):
        """Test query writes then reads."""
        connection.read_data = b"42\n"
        assert await mixin.query("MEAS?") == ["42"]
        assert connection.writes == [b"MEAS?\n"]

    @pytest.mark.asyncio
    async def test_query_passes_custom_terminator_to_read_and_write(self, mixin, connection):
        """Test query passes custom terminator to read and write."""
        connection.read_data = b"42\r\n"
        assert await mixin.query("MEAS?", scpi_terminator="\r\n") == ["42"]
        assert connection.writes == [b"MEAS?\r\n"]

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("0", Decimal("0")),
            ("1.23", Decimal("1.23")),
            ("-4.5", Decimal("-4.5")),
            ("1e-9", Decimal("1e-9")),
            ("9.91e37", Decimal("NaN")),
            ("9.9e37", Decimal("Infinity")),
            ("-9.9e37", Decimal("-Infinity")),
        ],
    )
    def test_map_scpi_number_to_decimal(self, value, expected):
        """Test map scpi number to decimal."""
        result = GenericScpiMixin._map_scpi_number_to_decimal(value)
        if expected.is_nan():
            assert result.is_nan()
        else:
            assert result == expected

    def test_map_scpi_number_to_decimal_invalid(self):
        """Test map scpi number to decimal invalid."""
        with pytest.raises(ValueError, match="is not a number"):
            GenericScpiMixin._map_scpi_number_to_decimal("not-a-number")

    @pytest.mark.asyncio
    async def test_read_number(self, mixin, connection):
        """Test read number."""
        connection.read_data = b"1.5,2.5,-3\n"
        assert list(await mixin.read_number()) == [Decimal("1.5"), Decimal("2.5"), Decimal("-3")]

    @pytest.mark.asyncio
    async def test_read_number_passes_custom_terminator(self, mixin, connection):
        """Test read number passes custom terminator."""
        connection.read_data = b"1.5,2.5\r\n"
        assert list(await mixin.read_number(scpi_terminator="\r\n")) == [Decimal("1.5"), Decimal("2.5")]

    @pytest.mark.asyncio
    async def test_query_number(self, mixin, connection):
        """Test query number."""
        connection.read_data = b"1.25,2.5\n"
        assert list(await mixin.query_number("MEAS?")) == [Decimal("1.25"), Decimal("2.5")]
        assert connection.writes == [b"MEAS?\n"]

    @pytest.mark.asyncio
    async def test_query_number_passes_custom_terminator(self, mixin, connection):
        """Test query number passes custom terminator."""
        connection.read_data = b"1.25,2.5\r\n"
        assert list(await mixin.query_number("MEAS?", "\r\n")) == [Decimal("1.25"), Decimal("2.5")]
        assert connection.writes == [b"MEAS?\r\n"]

    @pytest.mark.asyncio
    async def test_get_id(self, mixin):
        """Test get id."""
        mixin.query = AsyncMock(return_value=["Manufacturer", "Model", "12345", "1.2.3"])
        assert await mixin.get_id() == ("Manufacturer", "Model", "12345", "1.2.3")
        mixin.query.assert_awaited_once_with("*IDN?")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "response",
        [
            [],
            ["Manufacturer"],
            ["Manufacturer", "Model"],
            ["Manufacturer", "Model", "Serial"],
            ["Manufacturer", "Model", "Serial", "Revision", "Extra"],
        ],
    )
    async def test_get_id_rejects_invalid_id(self, mixin, response):
        """Test get id rejects invalid id."""
        mixin.query = AsyncMock(return_value=response)
        with pytest.raises(ValueError, match="Device returned invalid ID"):
            await mixin.get_id()

    @pytest.mark.asyncio
    async def test_wait_for_opc_returns_when_ready(self, mixin):
        """Test wait for opc returns when ready."""
        mixin.write = AsyncMock()
        mixin.read = AsyncMock(return_value=["1"])
        await mixin.wait_for_opc()
        mixin.write.assert_awaited_once_with("*OPC?")
        mixin.read.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_wait_for_opc_polls_until_ready(self, mixin, monkeypatch):
        """Test wait for opc polls until ready."""
        mixin.write = AsyncMock()
        mixin.read = AsyncMock(side_effect=[["0"], ["0"], ["1"]])
        sleep = AsyncMock()
        monkeypatch.setattr(asyncio, "sleep", sleep)
        await mixin.wait_for_opc()
        assert mixin.read.await_count == 3
        assert sleep.await_count == 2
        sleep.assert_awaited_with(0.1)

    @pytest.mark.asyncio
    async def test_wait_for_opc_honors_timeout(self, mixin):
        """Test wait for opc honors timeout."""
        mixin.write = AsyncMock()

        async def never_finishes():
            """Provide a test helper."""
            await asyncio.sleep(10)

        mixin.read = never_finishes
        with pytest.raises(asyncio.TimeoutError):
            await mixin.wait_for_opc(timeout=0.001)


class TestGenericScpiDriver:
    """Test TestGenericScpiDriver behavior."""

    def test_driver_name(self):
        """Test driver name."""
        assert GenericScpiDriver.driver() == "generic_scpi2"

    def test_initial_device_name(self, driver):
        """Test initial device name."""
        assert driver.device_name == "Generic SCPI device"

    @pytest.mark.asyncio
    async def test_enumerate_sets_device_name(self, driver):
        """Test enumerate sets device name."""
        driver.get_id = AsyncMock(return_value=("Acme", "SCPI-123", "SN42", "1.0"))
        await driver.enumerate()
        assert driver.device_name == "Acme SCPI-123 (SN42)"
        driver.get_id.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_enumerate_retries_after_invalid_id(self, driver):
        """Test enumerate retries after invalid id."""
        driver.get_id = AsyncMock(side_effect=[ValueError("invalid ID"), ("Acme", "SCPI-123", "SN42", "1.0")])
        await driver.enumerate()
        assert driver.get_id.await_count == 2
        assert driver.device_name == "Acme SCPI-123 (SN42)"

    @pytest.mark.asyncio
    async def test_enumerate_stops_after_two_failures(self, driver, caplog):
        """Test enumerate stops after two failures."""
        driver.get_id = AsyncMock(side_effect=ValueError("invalid ID"))
        with caplog.at_level("WARNING"):
            await driver.enumerate()
        assert driver.get_id.await_count == 2
        assert driver.device_name == "Generic SCPI device"
        assert "Could not query '*IDN?'" in caplog.text

    @pytest.mark.asyncio
    async def test_enumerate_does_not_retry_non_value_error(self, driver):
        """Test enumerate does not retry non value error."""
        driver.get_id = AsyncMock(side_effect=RuntimeError("connection failed"))
        with pytest.raises(RuntimeError, match="connection failed"):
            await driver.enumerate()
        driver.get_id.assert_awaited_once()
