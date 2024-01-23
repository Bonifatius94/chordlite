from __future__ import annotations
from dataclasses import dataclass, field
from typing import Union, Protocol
from hashlib import sha256


SHA256_KEYSPACE = 1 << 256


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


@dataclass(unsafe_hash=True)
class ResourceKey:
    value: int
    keyspace: int = SHA256_KEYSPACE

    def __post_init__(self):
        if self.value < 0:
            self.value += ((self.value // self.keyspace) + 1) * self.keyspace
        self.value = self.value % self.keyspace

    def __add__(self, other: Union[int, ChordKey]) -> ChordKey:
        other_value = other if isinstance(other, int) else other.value
        new_value = (self.value + other_value) % self.keyspace
        return ResourceKey(new_value, self.keyspace)

    def __sub__(self, other: Union[int, ChordKey]) -> ChordKey:
        other_value = other if isinstance(other, int) else other.value
        new_value = (self.value - other_value) % self.keyspace
        return ResourceKey(new_value, self.keyspace)

    def __mul__(self, other: Union[int, ChordKey]) -> ChordKey:
        other_value = other if isinstance(other, int) else other.value
        new_value = (self.value * other_value) % self.keyspace
        return ResourceKey(new_value, self.keyspace)

    def __lt__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other if isinstance(other, int) else other.value
        return self.value < other_value

    def __le__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other if isinstance(other, int) else other.value
        return self.value <= other_value

    def __gt__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other if isinstance(other, int) else other.value
        return self.value > other_value

    def __ge__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other if isinstance(other, int) else other.value
        return self.value >= other_value

    def __eq__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other if isinstance(other, int) else other.value
        return self.value == other_value

    def __neq__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other if isinstance(other, int) else other.value
        return self.value != other_value

    def __str__(self) -> str:
        return f"{self.value} (mod {self.keyspace})"

    def __repr__(self) -> str:
        return f"{self.value} (mod {self.keyspace})"


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
