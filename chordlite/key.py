from __future__ import annotations
from dataclasses import dataclass
from typing import Union, Protocol


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
