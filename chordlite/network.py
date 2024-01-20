from typing import Dict
from dataclasses import dataclass, field
from chordlite.node import ChordKey
from chordlite.transport import \
    ChordRequest, ChordResponse, ChordServer


@dataclass
class VirtualNetwork:
    nodes: Dict[ChordKey, ChordServer] = field(default_factory=dict)

    def register_node(self, node: ChordServer):
        self.nodes[node.node_id] = node

    def send_message(self, message: ChordRequest) -> ChordResponse:
        receiver = self.nodes[message.forward_id]
        response = receiver.process_message(message)
        return response


@dataclass
class HttpNetwork:
    def send_message(self, message: ChordRequest) -> ChordResponse:
        raise NotImplementedError()
