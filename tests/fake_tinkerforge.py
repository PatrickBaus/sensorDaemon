"""A fake connection to a Tinkerforge brick daemon, to use the real bricklet drivers of tinkerforge-async."""

import asyncio
from typing import Any
from uuid import UUID

from tinkerforge_async.ip_connection import Flags, HeaderPayload
from tinkerforge_async.ip_connection_helper import pack_payload


class FakeTinkerforgeConnection:
    """
    Replaces the `IPConnectionAsync` of a bricklet. It records the requests, answers a read of a callback
    configuration with the configuration that was set last, like a bricklet does, and delivers the callbacks
    registered with `callback()`.
    """

    def __init__(self) -> None:
        self.requests: list[tuple[str, bytes]] = []
        self.callback_configurations: dict[str, bytes] = {}
        self.__callbacks: asyncio.Queue[tuple[HeaderPayload, bytes]] = asyncio.Queue()

    async def send_request(
        self, device: Any, function_id: Any, data: bytes = b"", *, response_expected: bool = False
    ) -> tuple[HeaderPayload, bytes] | None:
        """Record the request. The reply to a GET_..._CALLBACK_CONFIGURATION is the data of the last SET_..."""
        name = function_id.name
        self.requests.append((name, bytes(data)))
        if name.startswith("SET_") and name.endswith("_CALLBACK_CONFIGURATION"):
            self.callback_configurations[name.removeprefix("SET_")] = bytes(data)
        if not response_expected:
            return None
        reply = self.callback_configurations.get(name.removeprefix("GET_"), b"") if name.startswith("GET_") else b""
        return HeaderPayload(device.uid, 1, True, function_id.value, Flags.OK), reply

    def callback(self, device: Any, callback_id: Any, form: str, *values: Any) -> None:
        """Deliver a callback packet of `device` with the values packed in the Tinkerforge format `form`."""
        header = HeaderPayload(device.uid, None, False, callback_id.value, Flags.OK)
        self.__callbacks.put_nowait((header, pack_payload(values, form)))

    async def read_events(self, uid: int):
        """Deliver the callbacks, then wait for more."""
        del uid  # all callbacks of the fake belong to the device under test
        while True:
            yield await self.__callbacks.get()

    def names_of_requests(self) -> list[str]:
        """The names of the functions called, in order."""
        return [name for name, _ in self.requests]


def tinkerforge_config(uid: int, uuid: UUID, interval: float = 1, on_connect: list | None = None) -> dict[str, Any]:
    """The config of a Tinkerforge bricklet with one sensor (sid 0), as it is served by the database."""
    return {
        "uuid": uuid,
        "uid": uid,
        "enabled": True,
        "label": "Test bricklet",
        "description": "",
        "config": {
            "0": {
                "interval": interval,
                "trigger_only_on_change": False,
                "description": "",
                "topic": "sensors/temperature",
                "unit": "K",
            }
        },
        "on_connect": on_connect or [],
    }
