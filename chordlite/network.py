from typing import Dict, Callable
from dataclasses import dataclass, field
from chordlite.node import ChordKey
from chordlite.transport import \
    ChordRequest, ChordResponse, NetworkedChordNode


def log_message(message: ChordRequest):
    print(f"{message.request_type}: {message.requester_id} -> {message.forward_id}")


@dataclass
class VirtualNetwork:
    nodes: Dict[ChordKey, NetworkedChordNode] = field(default_factory=dict)
    logger: Callable[[ChordRequest], None] = field(default=log_message)

    def register_node(self, node: NetworkedChordNode):
        self.nodes[node.node_id] = node

    def __call__(self, message: ChordRequest) -> ChordResponse:
        self.logger(message)
        receiver = self.nodes[message.forward_id]
        response = receiver.process_message(message)
        return response
