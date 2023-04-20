"""
This is a generic transport driver implementing the basic streams used by transports, that cannot enumerate its devices.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from aiostream import pipe, stream

from async_event_bus import event_bus
from errors import UnknownDriverError
from helper_functions import call_safely, context
from sensors.factories.sensor_factory import sensor_factory


class GenericTransport:
    """
    The transport base class for a generic connection.
    """

    @property
    def reconnect_interval(self) -> float:
        """
        Returns
        -------
        float
            The reconnect interval in seconds
        """
        return self.__reconnect_interval

    @property
    def uuid(self) -> UUID:
        """
        Returns
        -------
        UUID
            The unique identifier of the transport
        """
        return self.__uuid

    @property
    def name(self) -> str:
        """
        Returns
        -------
        str
            The name of the transport. Like Ethernet, GPIB, etc.
        """
        return self.__name

    @property
    def label(self) -> str:
        """
        Returns
        -------
        str
            A label as a human-readable descriptor of the transport.
        """
        return self.__label

    def __init__(  # pylint: disable=too-many-arguments  # The parameters are coming from a (relational) database
        self,
        uuid: UUID,
        database_topic: str,
        transport_name: str,
        reconnect_interval: float | None,
        label: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """

        Parameters
        ----------
        uuid: UUID
            The globally unique id of the transport as used by the database
        database_topic: str
            The database topic as used by the database driver to serve database queries
        transport_name: str
            The name or type of transport
        reconnect_interval: float or None
            The time in seconds between reconnection attempts. Use None if you want to use the default of 1 second.
        label: str
            A user defined label to distinguish the transport
        *args: Any
            Will be ignored
        **kwargs: Any
            Will be ignored
        """
        super().__init__(*args, **kwargs)
        self.__uuid = uuid
        self.__database_topic = database_topic
        self.__name = transport_name
        self.__reconnect_interval = reconnect_interval if reconnect_interval is not None else 1
        self.__label = label

    @staticmethod
    def _create_device(transport: Any, config: dict[str, Any]):
        if config is None:
            return None, None
        try:
            return config, sensor_factory.get(connection=transport, **config)
        except UnknownDriverError:
            logging.getLogger(__name__).warning("No driver available for device '%s'", config["driver"])
        except Exception:  # pylint: disable=broad-except
            # This is the catch-all in case there is a problem with the driver. We catch it here, because we do not want
            # The whole driver instance to blow up, because maybe it is a wrong settings, so we keep listening to
            # database updates.
            logging.getLogger(__name__).exception("Error while creating device '%s'", config["driver"])
        return None, None

    def _stream_data(self, transport):
        config_stream = (
            stream.chain(
                stream.call(
                    call_safely,
                    f"{self.__database_topic}/get_config",
                    f"{self.__database_topic}/status_update",
                    transport.uuid,
                ),
                stream.iterate(event_bus.subscribe(f"nodes/by_uuid/{transport.uuid}/add")),
            )
            | pipe.map(lambda config: self._create_device(transport, config))
            | pipe.starmap(lambda config, device: stream.empty() if device is None else device.stream_data(config))
            | pipe.switch()
            | context.pipe(
                transport,
                on_enter=lambda: logging.getLogger(__name__).info(
                    "Connected to %s at %s (%s).", transport.name, transport.uri, transport.label
                ),
                on_exit=lambda exit_code: logging.getLogger(__name__).info(
                    "Disconnected from %s at %s (%s). Reason: %s.",
                    transport.name,
                    transport.uri,
                    transport.label,
                    exit_code,
                ),
            )
        )
        return config_stream
