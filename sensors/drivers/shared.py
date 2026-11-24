"""
This file contains all shared classes used by the drivers.
"""

from dataclasses import dataclass


@dataclass
class DeviceStatus:
    """The full status of a device that is managed by Kraken."""
