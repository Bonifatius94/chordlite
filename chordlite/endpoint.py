import socket
from dataclasses import dataclass, field
from typing import Union
from hashlib import sha256
from chordlite.key import \
    SHA256_KEYSPACE, ChordKey, ResourceKey


@dataclass
class IPEndpointId:
    ip_address: str
    port: str
    keyspace: int = SHA256_KEYSPACE
    key: ChordKey = field(init=False)

    def __post_init__(self):
        node_name = f"{self.ip_address}:{self.port}"
        node_hash = sha256(node_name.encode("ascii"))
        value = int(node_hash.hexdigest(), 16)
        self.key = ResourceKey(value, self.keyspace)

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


def local_endpoint(chord_port: str) -> IPEndpointId:
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    return IPEndpointId(ip_address, chord_port)
