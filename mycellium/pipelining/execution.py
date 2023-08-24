"""
Execution Module
================

"""

import time
import queue
import multiprocessing

from .pipeline import Pipeline
from .message import Message


class Executor(multiprocessing.Process):
    def __init__(self, stop_event: multiprocessing.Event, task_queue: multiprocessing.JoinableQueue, debug: bool = False) -> None:
        super().__init__()

        self.__debug = debug

        self.__task_queue = task_queue
        self.__stop_event = stop_event

    def run(self) -> None:
        while not self.__stop_event.is_set():
            try:
                # Try fetching a task item off the queue and process it
                task_data: dict = self.__task_queue.get(timeout=1)
                message_data: dict = task_data["message"]
                structure_data: dict = task_data["structure"]
                message = Message(**message_data)
                received_time = 0
                if self.__debug:
                    print(f"-> Consuming task with message: {message}")
                    received_time = time.time()
                    received_latency = received_time - message.timestamp
                    print(f"Received latency: {received_latency * 1_000} ms")

                # Build the pipeline and execute it with the given message
                pipeline = Pipeline.from_dict(structure_data)
                pipeline.execute(message)

                if self.__debug:
                    processed_time = time.time()
                    pipeline_processing_time = processed_time - received_time
                    total_processing_time = processed_time - message.timestamp
                    print(f"<- Consumed task with message: {message}")
                    print(f"Pipeline time to processes: {pipeline_processing_time * 1_000} ms")
                    print(f"Total time to processes: {total_processing_time * 1_000} ms")

                self.__task_queue.task_done()
            except queue.Empty:
                pass
