"""
Switch Node Module
==================

"""

from enum import Enum
from dataclasses import dataclass

from .node import Node
from ..message import Message, Payload


class _Comparison(Enum):
    EQUALS = "eq"
    NOT_EQUALS = "ne"
    AND = "and"
    OR = "or"
    LESS_THAN = "lt"
    LESS_THAN_OR_EQUAL = "le"
    GREATER_THAN = "gt"
    GREATER_THAN_OR_EQUAL = "ge"


@dataclass(frozen=True)
class _Condition:
    comparison: _Comparison
    value: None | int | str | float | bool


class SwitchNode(Node):
    def __init__(self, output_ports: list[str], config: dict) -> None:
        super().__init__(output_ports)
        config_conditions: dict = config["conditions"]
        self.__output_conditions: dict[str, _Condition] = {
            output_port: _Condition(comparison=_Comparison(condition["comparison"]), value=condition["value"])
            for output_port, condition in config_conditions.items()
        }

    def process(self, message: Message) -> tuple[set[str], Payload]:
        survived_children = set()
        for output_port, condition in self.__output_conditions.items():
            if self.__conditions_is_met(condition, message.payload):
                survived_children = survived_children.union(self.port_children(output_port))
        return survived_children, message.payload

    @staticmethod
    def __conditions_is_met(condition: _Condition, payload: Payload) -> bool:
        left_value = payload
        right_value = condition.value

        # Evaluate condition, if any of the cases are false, do not allow the node's predecessors to execute.
        match condition.comparison:
            case _Comparison.EQUALS:
                return left_value == right_value
            case _Comparison.NOT_EQUALS:
                return left_value != right_value
            case _Comparison.AND:
                return left_value and right_value
            case _Comparison.OR:
                return left_value or right_value
            case _Comparison.LESS_THAN:
                return left_value < right_value
            case _Comparison.LESS_THAN_OR_EQUAL:
                return left_value <= right_value
            case _Comparison.GREATER_THAN:
                return left_value > right_value
            case _Comparison.GREATER_THAN_OR_EQUAL:
                return left_value >= right_value
            case _:
                raise ValueError(f"Unknown condition comparison provided: '{condition.comparison}'")
