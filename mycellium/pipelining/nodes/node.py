"""
Node Module
===========

"""

from abc import ABC, abstractmethod

from ..message import Message, Payload


class Node(ABC):
    def __init__(self, output_ports: list[str], config: None | dict = None) -> None:
        self.__outputs: dict[str, set[str]] = {output_port: set() for output_port in output_ports}
        self.__parents: set[str] = set()

    @abstractmethod
    def process(self, message: Message) -> tuple[set[str], Payload]:
        pass

    @property
    def num_outputs(self) -> int:
        return len(self.__outputs)

    @property
    def children(self) -> set[str]:
        return set().union(*self.__outputs.values())

    @property
    def parents(self) -> set[str]:
        return self.__parents

    def port_children(self, port: str) -> set[str]:
        return self.__outputs[port]

    def add_child(self, output_port: str, child_label: str) -> None:
        self.__outputs[output_port].add(child_label)

    def add_parent(self, parent_label: str) -> None:
        self.__parents.add(parent_label)


