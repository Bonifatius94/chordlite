from typing import Dict, Callable
from dataclasses import dataclass, field
from random import randint
from time import sleep
from chordlite.node import ChordKey
from chordlite.transport import \
    ChordRequest, ChordResponse, NetworkedChordNode


def log_message(message: ChordRequest):
    print(f"{message.request_type}: {message.requester_id} -> {message.forward_id}")


@dataclass
class VirtualNetwork:
    nodes: Dict[ChordKey, NetworkedChordNode] = field(default_factory=dict)
    logger: Callable[[ChordRequest], None] = field(default=lambda m: None)

    def register_node(self, node: NetworkedChordNode):
        self.nodes[node.node_id] = node

    def __call__(self, message: ChordRequest) -> ChordResponse:
        self.logger(message)
        # TODO: add random latency to harden the protocol
        # TODO: add message loss to harden the protocol
        # latency = randint(1, 10) / 1000
        # sleep(latency)
        receiver = self.nodes[message.forward_id]
        response = receiver.process_message(message)
        return response
