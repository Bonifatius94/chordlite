from __future__ import annotations
from enum import IntEnum
from typing import List, Tuple, Optional, Protocol
from dataclasses import dataclass, field
from math import log2, ceil
from chordlite.key import ChordKey


class ChordStatus(IntEnum):
    SUCCESS = 0
    FAILURE = 1
    TIMEOUT = 2


@dataclass
class ChordEndpoint(Protocol):
    node_id: ChordKey
    successor: ChordEndpoint

    def find_successor(self, key: ChordKey) -> ChordEndpoint:
        raise NotImplementedError()

    def find_predecessor(self, key: ChordKey) -> ChordEndpoint:
        raise NotImplementedError()

    def challenge_join(self, joining_node: ChordEndpoint) -> Tuple[ChordStatus, ChordEndpoint]:
        raise NotImplementedError()

    def notify(self, new_successor: ChordEndpoint) -> ChordStatus:
        raise NotImplementedError()


@dataclass
class ChordNode:
    node_id: ChordKey
    predecessor: ChordEndpoint = field(default=None)
    fingers: List[ChordEndpoint] = field(init=False)
    finger_starts: List[ChordKey] = field(init=False)

    def __post_init__(self):
        self.fingers = [self for _ in range(self.m)]
        self.finger_starts = [self.node_id + 2**i for i in range(self.m)]
        self.predecessor = None

    @property
    def successor(self) -> ChordEndpoint:
        return self.fingers[0]

    @property
    def keyspace(self) -> int:
        return self.node_id.keyspace

    @property
    def m(self) -> int:
        return int(ceil(log2(self.keyspace)))

    @property
    def is_uninitialized(self) -> bool:
        return self.successor.node_id == self.node_id

    def find_successor(self, key: ChordKey) -> ChordEndpoint:
        pred = self.find_predecessor(key)
        return pred.successor

    def find_predecessor(self, key: ChordKey) -> ChordEndpoint:
        if self.is_uninitialized:
            return self
        elif key - self.node_id <= self.successor.node_id - self.node_id:
            return self
        else:
            forward = self.closest_preceding_finger(key)
            return forward.find_predecessor(key)

    def closest_preceding_finger(self, key: ChordKey) -> ChordEndpoint:
        return max(self.fingers, key=lambda f: f.node_id - key)

    def update_finger_table(self, bootstrap: Optional[ChordEndpoint]=None):
        forward = bootstrap if bootstrap else self
        for i, key in enumerate(self.finger_starts):
            self.fingers[i] = forward.find_successor(key)

    def initiate_join(self, bootstrap: ChordEndpoint):
        if bootstrap.node_id != self.node_id:
            self.predecessor = self
            new_successor = bootstrap.find_successor(self.node_id)
            status, new_predecessor = new_successor.challenge_join(self)
            # TODO: add error handling

            for i in range(len(self.fingers)):
                self.fingers[i] = new_successor
            self.update_finger_table(new_successor)

            self.predecessor = new_predecessor
            if self.predecessor != self.successor:
                status = self.predecessor.notify(self)
                # TODO: add error handling

    def challenge_join(self, joining_node: ChordEndpoint) -> Tuple[ChordStatus, ChordEndpoint]:
        old_predecessor = self if self.is_uninitialized else self.predecessor
        self.predecessor = joining_node
        if self.is_uninitialized:
            for i in range(len(self.fingers)):
                self.fingers[i] = joining_node
        else:
            self.update_finger_table()
        return ChordStatus.SUCCESS, old_predecessor

    def notify(self, new_successor: ChordEndpoint) -> ChordStatus:
        old_successor = self.fingers[0]
        if new_successor.node_id - self.node_id < old_successor.node_id - self.node_id:
            self.fingers[0] = new_successor
            self.update_finger_table()
        return ChordStatus.SUCCESS
