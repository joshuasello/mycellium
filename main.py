""" main.py

"""


from mycellium import *


def main() -> None:
    # pipeline_structure = fetch_pipeline_structure(pipeline_id=0)
    # pipeline = build_pipeline(pipeline_structure.structure)
    # message = NodeMessage(payload=3)
    # pipeline.execute(message)
    num_consumers = 3

    task_queue = multiprocessing.JoinableQueue()
    stop_event = multiprocessing.Event()

    producer = MQTTIngestor(stop_event, task_queue, client_id=0, broker_host="localhost", broker_port=1883)
    consumers = [Executor(stop_event, task_queue) for _ in range(num_consumers)]

    producer.start()
    for consumer in consumers:
        consumer.start()

    try:
        while True:
            time.sleep(1)  # Keep the main process running
    except KeyboardInterrupt:
        print("Stopping processes...")
        stop_event.set()

    producer.join()

    for consumer in consumers:
        consumer.join()

    print("All processes have completed.")


if __name__ == '__main__':
    main()

