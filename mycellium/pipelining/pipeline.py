"""
Pipeline Module
===============

"""

from .nodes import *


class Pipeline:
    def __init__(self, starting_node_labels: set[str]) -> None:
        self.__starting_node_labels = starting_node_labels
        self.__nodes: dict[str, Node] = {}

    @staticmethod
    def from_dict(structure: dict) -> "Pipeline":
        starting_nodes: set[str] = set(structure["starting_nodes"])
        pipline = Pipeline(starting_node_labels=starting_nodes)
        # 1. Add nodes by there associated object types
        node_descriptions: dict = structure["nodes"]
        for node_label, node_description in node_descriptions.items():
            # Get data from node description
            node_output_ports: list[str] = node_description["output_ports"]
            node_type: str = node_description["type"]
            node_config: dict = node_description["config"]
            # Initialise node and add it to the pipeline
            node = build_node(node_type=node_type, output_ports=node_output_ports, config=node_config)
            pipline.add_node(node_label, node)

        # 2. Connect nodes
        connection_descriptions: list[dict] = structure["connections"]
        for connection_description in connection_descriptions:
            parent_label = connection_description["parent"]
            child_label = connection_description["child"]
            port = connection_description["port"]
            pipline.connect_nodes(from_node_port=port, from_node_label=parent_label, to_node_label=child_label)
        return pipline

    def add_node(self, node_label: str, node: Node) -> None:
        self.__nodes[node_label] = node

    def connect_nodes(self, from_node_port: str, from_node_label: str, to_node_label: str) -> None:
        # Insert node edges, ensuring that each insertion maintains that the graph is a
        # directed acyclic graph (DAG). If a path currently exists from the destination node
        # to the source node, adding the proposed edge will cause a cycle, which is not permitted,
        if self.__path_exists(to_node_label, from_node_label):
            raise ValueError(f"Edge '{from_node_label}' -> '{to_node_label}' could not be added since it creates a "
                             f"cycle.")
        self.__nodes[from_node_label].add_child(from_node_port, to_node_label)
        self.__nodes[to_node_label].add_parent(from_node_label)

    def execute(self, message: Message) -> None:
        # Stores the execution results of the executed ports:
        execution_results: dict[str] = {}
        # Stores the keys of the nodes to execute for the current iteration.
        generation_to_process = self.__starting_node_labels
        # Stores the keys of the nodes that are waiting on dependent nodes to execute.
        waiting_node_labels = set()

        # Store propagating the message through the network
        while True:
            survived_children = set()
            for node_label in generation_to_process:
                parents = self.__nodes[node_label].parents
                payload = None
                match len(parents):
                    case 0:
                        payload = message.payload
                    case 1:
                        parent = parents.pop()
                        payload = execution_results[parent]
                    case _:
                        payload = []
                        # Combine parent payloads into a list
                        for parent in parents:
                            payload.append(execution_results[parent])
                        message.payload = payload
                survived_child_labels, next_payload = self.__nodes[node_label].process(message)
                execution_results[node_label] = next_payload
                survived_children = survived_children.union(survived_child_labels)

            # For each candidate node, check that all its dependent nodes (parents)
            # have already executed. If they have, add the candidate to the execution nodes for
            # the next iteration. Otherwise, add to delayed execution nodes.
            candidates = survived_children.union(waiting_node_labels)
            waiting_node_labels.clear()
            generation_to_process.clear()

            for candidate in candidates:
                unexecuted_parents = self.__nodes[candidate].parents - execution_results.keys()
                has_unexecuted_parents = len(unexecuted_parents) != 0

                if has_unexecuted_parents:
                    waiting_node_labels.add(candidate)
                else:
                    generation_to_process.add(candidate)

            # Finally, if there are no more nodes to execute on the next iteration, break out of the
            # loop.
            if len(generation_to_process) == 0:
                break

    def __path_exists(self, from_node_label: str, to_node_label: str) -> bool:
        return self.__depth_first_search(from_node_label, to_node_label, set())

    def __depth_first_search(self, from_node_label: str, to_node: str, visited: set[str]) -> bool:
        visited.add(from_node_label)

        for neighbor in self.__nodes[from_node_label].children:
            if neighbor == to_node:
                return True
            if neighbor not in visited and self.__depth_first_search(neighbor, to_node, visited):
                return True
        return False
