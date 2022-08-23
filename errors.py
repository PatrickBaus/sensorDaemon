"""
This file contains all custom errors thrown by Kraken.
"""


class UnknownDriverError(ValueError):
    """Raised, when an unknown driver is requested from the factory"""


class ConfigurationError(Exception):
    """Raised, when either a requested function call cannot be found in the driver."""


class SensorNotReady(Exception):
    """Raised, when the sensor is not yet ready to provide data."""
