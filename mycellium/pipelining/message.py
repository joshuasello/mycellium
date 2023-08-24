"""
Node Message Module
===================

"""

from dataclasses import dataclass


Payload = None | str | int | float | bool | dict | list


@dataclass
class Message:
    timestamp: float
    payload: Payload
