"""
Nodes Subpackage
================

"""

from typing import Type

from .debug import *
from .mqtt import *
from .node import *
from .switch import *


_types: dict[str, Type[Node]] = {
    "debug": DebugNode,
    "switch": SwitchNode,
    "mqtt": MQTTPublisherNode
}


def build_node(node_type: str, output_ports: list[str], config: dict) -> Node:
    return _types[node_type](output_ports, config)
