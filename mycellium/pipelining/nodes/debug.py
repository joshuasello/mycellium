"""
Debug Node Module
=================

"""

from .node import Node
from ..message import Message, Payload


class DebugNode(Node):
    def __init__(self, output_ports: list[str], config: dict) -> None:
        super().__init__(output_ports)
        config_output: str = config["output"]
        config_show_payload: bool = config["show_payload"]
        self.__output = config_output
        self.__show_payload = config_show_payload

    def process(self, message: Message) -> tuple[set[str], Payload]:
        print(self.__output)
        if self.__show_payload:
            print(f"Payload: {message.payload}")
        return self.children, message.payload
