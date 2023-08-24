"""
Storage Module
========================

"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ClientRecord:
    id: int
    workspace_id: 0
    broker_host: str
    broker_port: int


@dataclass(frozen=True)
class SubscriptionRecord:
    id: int
    client_id: int
    topic: str
    qos: int


@dataclass(frozen=True)
class PipelineRecord:
    id: int
    structure: dict


# --------------------------------------------------
#   Workspace
# --------------------------------------------------

def fetch_workspaces() -> list:
    pass


def fetch_workspace(workspace_id: int) -> None:
    pass


def create_workspace() -> None:
    pass


def delete_workspace() -> None:
    pass


def update_workspace() -> None:
    pass


# --------------------------------------------------
#   Ingestion - MQTT
# --------------------------------------------------

def fetch_clients(workspace_id: int) -> list[ClientRecord]:
    clients: list[ClientRecord] = list(
        filter(lambda config: config.workspace_id == workspace_id, _mqtt_clients))
    return clients


def fetch_client(client_id: int) -> None | ClientRecord:
    pass


def create_client() -> None:
    pass


def delete_client() -> None:
    pass


def update_client() -> None:
    pass


def fetch_subscriptions(client_id: int) -> list[SubscriptionRecord]:
    subscriptions: list[SubscriptionRecord] = list(
        filter(lambda config: config.client_id == client_id, _mqtt_subscriptions))
    return subscriptions


def fetch_subscription(client_id: int, topic: str) -> None | SubscriptionRecord:
    for subscription in _mqtt_subscriptions:
        if subscription.client_id == client_id and subscription.topic == topic:
            return subscription

    return None


def create_subscription(client_id: int, topic: str, qos: int) -> None:
    new_id = 0 if len(_mqtt_subscriptions) == 0 else _mqtt_subscriptions[-1].id + 1
    _mqtt_subscriptions.append(SubscriptionRecord(id=new_id, client_id=client_id, topic=topic, qos=qos))


def delete_subscription(subscription_id: int) -> None:
    pass


def update_subscription(subscription_id: int) -> None:
    pass


def fetch_subscription_pipeline(subscription_id: int) -> PipelineRecord:
    pipeline_id = _mqtt_subscription_pipelines[subscription_id]
    return fetch_pipeline(pipeline_id)


# --------------------------------------------------
#   Pipelining
# --------------------------------------------------

def fetch_pipeline(pipeline_id: int) -> None | PipelineRecord:
    for pipeline_structure in _pipeline_structures:
        if pipeline_structure.id == pipeline_id:
            return pipeline_structure
    return None


_mqtt_clients = [
    ClientRecord(id=0, workspace_id=0, broker_host="localhost", broker_port=1883)
]

_mqtt_subscriptions = [
    SubscriptionRecord(id=0, client_id=0, topic="topic/hello", qos=0)
]

_mqtt_subscription_pipelines = {
    0: 0
}


_pipeline_structures: list[PipelineRecord] = [
    PipelineRecord(
        id=0,
        structure={
            "starting_nodes": ["switch0"],
            "nodes": {
                "switch0": {
                    "output_ports": ["1", "2", "3"],
                    "type": "switch",
                    "config": {
                        "conditions": {
                            "1": {
                                "comparison": "eq",
                                "value": 1
                            },
                            "2": {
                                "comparison": "eq",
                                "value": 2
                            },
                            "3": {
                                "comparison": "eq",
                                "value": "apples"
                            }
                        }
                    }
                },
                "debug0": {
                    "output_ports": [],
                    "type": "debug",
                    "config": {
                        "output": "Hello from debug0",
                        "show_payload": True
                    }
                },
                "debug1": {
                    "output_ports": [],
                    "type": "debug",
                    "config": {
                        "output": "Hello from debug1",
                        "show_payload": True
                    }
                },
                "mqtt0": {
                    "output_ports": [],
                    "type": "mqtt",
                    "config": {
                        "broker_host": "localhost",
                        "broker_port": 1883,
                        "topic": "topic/example",
                        "qos": 0
                    }
                }
            },
            "connections": [
                {
                    "parent": "switch0",
                    "child": "debug0",
                    "port": "1",
                },
                {
                    "parent": "switch0",
                    "child": "debug1",
                    "port": "2",
                },
                {
                    "parent": "switch0",
                    "child": "mqtt0",
                    "port": "3",
                }
            ]
        }
    )
]

