# -*- coding: utf-8 -*-
"""
This file contains all custom errors thrown by Kraken.
"""


class DisconnectedDuringConnectError(Exception):
    """
    Thrown if a connection is canceled during connect
    """
