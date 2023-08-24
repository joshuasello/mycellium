"""
MQTT Publisher Node Module
==========================

"""

import paho.mqtt.client as mqtt

from .node import Node
from ..message import Message, Payload


class MQTTPublisherNode(Node):
    def __init__(self, output_ports: list[str], config: dict) -> None:
        super().__init__(output_ports)

        self.__broker_host: str = config["broker_host"]
        self.__broker_port = int(config["broker_port"])
        self.__topic: str = config["topic"]
        self.__qos = int(config["qos"])

        self.__client = mqtt.Client()

    def process(self, message: Message) -> tuple[set[str], Payload]:
        # Connect to the broker
        self.__client.connect(self.__broker_host, self.__broker_port)
        # Publish a message
        self.__client.publish(self.__topic, message.payload, qos=self.__qos)
        # Disconnect from the broker
        self.__client.disconnect()

        return self.children, message.payload


