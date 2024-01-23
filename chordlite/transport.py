from __future__ import annotations
from enum import IntEnum
from typing import Tuple, Union, Protocol, Callable
from dataclasses import dataclass, field
from hashlib import sha256
from time import sleep

from chordlite.key import ChordKey as RawChordKey
from chordlite.node import ChordNode, ChordStatus, ChordEndpoint


class ChordRequestType(IntEnum):
    FIND_PRED = 0
    FIND_SUCC = 1
    JOIN = 2
    NOTIFY = 3
    SUCC_LOOKUP = 4


class ChordKey(Protocol):
    @property
    def value(self) -> int:
        raise NotImplementedError()

    @property
    def keyspace(self) -> int:
        raise NotImplementedError()

    def __add__(self, other: Union[int, ChordKey]) -> ChordKey:
        raise NotImplementedError()

    def __sub__(self, other: Union[int, ChordKey]) -> ChordKey:
        raise NotImplementedError()

    def __mul__(self, other: Union[int, ChordKey]) -> ChordKey:
        raise NotImplementedError()

    def __lt__(self, other: Union[int, ChordKey]) -> bool:
        raise NotImplementedError()

    def __le__(self, other: Union[int, ChordKey]) -> bool:
        raise NotImplementedError()

    def __gt__(self, other: Union[int, ChordKey]) -> bool:
        raise NotImplementedError()

    def __ge__(self, other: Union[int, ChordKey]) -> bool:
        raise NotImplementedError()

    def __eq__(self, other: Union[int, ChordKey]) -> bool:
        raise NotImplementedError()

    def __neq__(self, other: Union[int, ChordKey]) -> bool:
        raise NotImplementedError()

    def __str__(self) -> str:
        raise NotImplementedError()

    def __repr__(self) -> str:
        raise NotImplementedError()


@dataclass
class IPEndpointId:
    ip_address: str
    port: str
    keyspace: int = 1 << 256
    key: ChordKey = field(init=False)

    def __post_init__(self):
        node_name = f"{self.ip_address}:{self.port}"
        hash = sha256(node_name.encode("ascii"))
        value = int(hash.hexdigest(), 16)
        self.key = RawChordKey(value, self.keyspace)

    @property
    def value(self) -> int:
        return self.key.value

    def __add__(self, other: Union[int, ChordKey]) -> ChordKey:
        return self.key + other

    def __sub__(self, other: Union[int, ChordKey]) -> ChordKey:
        return self.key - other

    def __mul__(self, other: Union[int, ChordKey]) -> ChordKey:
        return self.key * other

    def __lt__(self, other: Union[int, ChordKey]) -> bool:
        return self.key < other

    def __le__(self, other: Union[int, ChordKey]) -> bool:
        return self.key <= other

    def __gt__(self, other: Union[int, ChordKey]) -> bool:
        return self.key > other

    def __ge__(self, other: Union[int, ChordKey]) -> bool:
        return self.key >= other

    def __eq__(self, other: Union[int, ChordKey]) -> bool:
        return self.key == other

    def __neq__(self, other: Union[int, ChordKey]) -> bool:
        return self.key != other

    def __hash__(self):
        return self.key.__hash__()

    def __str__(self) -> str:
        return f"{self.ip_address}:{self.port}"

    def __repr__(self) -> str:
        return f"{self.ip_address}:{self.port}"


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
