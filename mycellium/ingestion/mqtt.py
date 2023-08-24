""" mqtt.py

"""


from ..storage import *

import time
import random
import multiprocessing
import paho.mqtt.client as mqtt


class MQTTIngestor(multiprocessing.Process):
    def __init__(self, stop_event: multiprocessing.Event, task_queue: multiprocessing.JoinableQueue, client_id: int, broker_host: str, broker_port: int) -> None:
        super().__init__()

        self.__task_queue = task_queue
        self.__stop_event = stop_event

        self.__client_id = client_id
        self.__broker_host = broker_host
        self.__broker_port = broker_port
        self.__client: None | mqtt.Client = None
        self.__is_connected = False

    def run(self) -> None:
        self.__client = mqtt.Client()

        self.__client.on_connect = self.__on_connect
        self.__client.on_message = self.__on_message

        self.__client.connect(self.__broker_host, self.__broker_port)

        # Start the client's event loop on a new thread.
        self.__client.loop_start()

        try:
            while not self.__stop_event.is_set():
                pass
        except KeyboardInterrupt:
            pass
        print(f"Exiting client {self.__client_id}...")

        # Stop the client's event loop before exiting
        self.__client.loop_stop()
        self.__is_connected = False

    def __on_connect(self, client: mqtt.Client, userdata, flags, rc) -> None:
        if rc == 0:
            self.__is_connected = True
            self.__update_subscriptions()
        else:
            raise RuntimeError(f"Client '{self.__client_id}' connection failed with result code {rc}")

    def __on_message(self, client: mqtt.Client, userdata, message) -> None:
        topic = message.topic
        payload = message.payload.decode()

        print(f"-> Ingested from topic '{topic}' payload {payload}")
        subscription = fetch_subscription(client_id=self.__client_id, topic=topic)
        pipeline_record = fetch_subscription_pipeline(subscription.id)
        message_data = {
            "timestamp": time.time(),
            "payload": payload
        }
        task_data = {
            "message": message_data,
            "structure": pipeline_record.structure
        }

        # Add the new task item to the task queue
        self.__task_queue.put(task_data)
        time.sleep(random.uniform(0.1, 0.5))

    def __update_subscriptions(self) -> None:
        if not self.__is_connected:
            return

        # Add subscriptions to client
        for subscription in fetch_subscriptions(client_id=self.__client_id):
            print(f"Subscribed to {subscription}")
            self.__client.subscribe(topic=subscription.topic, qos=subscription.qos)
