from __future__ import annotations
from enum import IntEnum
from typing import Tuple, Callable
from dataclasses import dataclass, field
from time import sleep

from chordlite.key import IPEndpointId, ChordKey
from chordlite.node import ChordNode, ChordStatus, ChordEndpoint


class ChordRequestType(IntEnum):
    FIND_PRED = 0
    FIND_SUCC = 1
    JOIN = 2
    NOTIFY = 3
    SUCC_LOOKUP = 4


@dataclass
class ChordRequest:
    request_type: ChordRequestType
    forward_id: IPEndpointId
    receiver_id: IPEndpointId
    requester_id: IPEndpointId
    requested_resource_id: ChordKey
    new_successor_id: IPEndpointId = field(default=None)
    new_predecessor_id: IPEndpointId = field(default=None)


@dataclass
class ChordResponse:
    responder_id: IPEndpointId
    successor_id: IPEndpointId = field(default=None)
    predecessor_id: IPEndpointId = field(default=None)
    status: ChordStatus = field(default=ChordStatus.SUCCESS)


RequestSender = Callable[[ChordRequest], ChordResponse]


@dataclass
class ChordRemoteEndpoint:
    local_id: IPEndpointId
    remote_id: IPEndpointId
    network: RequestSender

    @property
    def node_id(self) -> ChordKey:
        return self.remote_id

    @property
    def successor(self) -> ChordEndpoint:
        request = ChordRequest(
            ChordRequestType.SUCC_LOOKUP,
            self.remote_id,
            self.local_id,
            self.remote_id,
            self.remote_id
        )
        response = self.network(request)
        endpoint = ChordRemoteEndpoint(
            self.local_id, response.successor_id, self.network
        )
        return endpoint

    def find_successor(self, key: ChordKey) -> ChordEndpoint:
        request = ChordRequest(
            ChordRequestType.FIND_SUCC,
            self.remote_id,
            self.local_id,
            self.local_id,
            key
        )
        response = self.network(request)
        endpoint = ChordRemoteEndpoint(
            self.local_id, response.successor_id, self.network
        )
        return endpoint

    def find_predecessor(self, key: ChordKey) -> ChordEndpoint:
        request = ChordRequest(
            ChordRequestType.FIND_SUCC,
            self.remote_id,
            self.local_id,
            self.local_id,
            key
        )
        response = self.network(request)
        endpoint = ChordRemoteEndpoint(
            self.local_id, response.predecessor_id, self.network
        )
        return endpoint

    def challenge_join(self, joining_node: ChordEndpoint) -> Tuple[ChordStatus, ChordEndpoint]:
        request = ChordRequest(
            ChordRequestType.FIND_SUCC,
            self.remote_id,
            self.remote_id,
            self.local_id,
            self.local_id
        )
        response = self.network(request)
        endpoint = ChordRemoteEndpoint(
            self.local_id, response.predecessor_id, self.network
        )
        return response.status, endpoint

    def notify(self, new_successor: ChordEndpoint) -> ChordStatus:
        request = ChordRequest(
            ChordRequestType.FIND_SUCC,
            self.remote_id,
            self.remote_id,
            self.local_id,
            self.local_id
        )
        response = self.network(request)
        return response.status


@dataclass
class ChordServer:
    network: RequestSender
    local: ChordNode

    @property
    def node_id(self) -> ChordKey:
        return self.local.node_id

    def process_message(self, message: ChordRequest) -> ChordResponse:
        local_id: IPEndpointId = self.local.node_id
        if message.request_type == ChordRequestType.SUCC_LOOKUP:
            response = ChordResponse(local_id, successor_id=self.local.successor.node_id)
            return response
        elif message.request_type == ChordRequestType.FIND_PRED:
            pred = self.local.find_predecessor(message.requested_resource_id)
            response = ChordResponse(local_id, predecessor_id=pred.node_id)
            return response
        elif message.request_type == ChordRequestType.FIND_SUCC:
            succ = self.local.find_successor(message.requested_resource_id)
            response = ChordResponse(local_id, successor_id=succ.node_id)
            return response
        elif message.request_type == ChordRequestType.JOIN:
            joining_node = ChordRemoteEndpoint(local_id, message.requester_id, self.network)
            status, pred = self.local.challenge_join(joining_node)
            response = ChordResponse(local_id, predecessor_id=pred.node_id, status=status)
            return response
        elif message.request_type == ChordRequestType.NOTIFY:
            joining_node = ChordRemoteEndpoint(local_id, message.requester_id, self.network)
            status = self.local.notify(joining_node)
            response = ChordResponse(local_id, status=status)
            return response
        else:
            raise RuntimeError("Unsupported request type!")


@dataclass
class NetworkedChordNode:
    node_id: IPEndpointId
    network: RequestSender
    finger_update_interval_secs: float = 5.0
    node: ChordNode = field(init=False)
    server: ChordServer = field(init=False)

    def __post_init__(self):
        self.node = ChordNode(self.node_id)
        self.server = ChordServer(self.network, self.node)

    def join_network(self, bootstrap: ChordEndpoint):
        self.node.initiate_join(bootstrap)

        # TODO: periodically update finger table in background task
        sleep(self.finger_update_interval_secs)
        self.node.update_finger_table()

    def lookup(self, key: ChordKey) -> ChordEndpoint:
        return self.node.find_successor(key)

    # TODO: add http server receiving request as background task
    def process_message(self, message: ChordRequest) -> ChordResponse:
        return self.server.process_message(message)
