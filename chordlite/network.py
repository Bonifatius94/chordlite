from typing import Dict
from dataclasses import dataclass, field
from chordlite.node import ChordKey
from chordlite.transport import \
    ChordRequest, ChordResponse, NetworkedChordNode


@dataclass
class VirtualNetwork:
    nodes: Dict[ChordKey, NetworkedChordNode] = field(default_factory=dict)

    def register_node(self, node: NetworkedChordNode):
        self.nodes[node.node_id] = node

    def __call__(self, message: ChordRequest) -> ChordResponse:
        print(f"{message.request_type}: {message.requester_id} -> {message.forward_id}")
        receiver = self.nodes[message.forward_id]
        response = receiver.process_message(message)
        return response


# @dataclass
# class HttpNetwork:
#     def send_message(self, message: ChordRequest) -> ChordResponse:
#         raise NotImplementedError()
