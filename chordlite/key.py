from __future__ import annotations
from dataclasses import dataclass
from typing import Union


@dataclass
class ChordKey:
    value: int
    keyspace: int

    def __post_init__(self):
        if self.value < 0:
            self.value += ((self.value // self.keyspace) + 1) * self.keyspace
        self.value = self.value % self.keyspace

    def __add__(self, other: Union[int, ChordKey]) -> ChordKey:
        other_value = other.value if type(other) == ChordKey else other
        new_value = (self.value + other_value) % self.keyspace
        return ChordKey(new_value, self.keyspace)

    def __sub__(self, other: Union[int, ChordKey]) -> ChordKey:
        other_value = other.value if type(other) == ChordKey else other
        new_value = (self.value - other_value) % self.keyspace
        return ChordKey(new_value, self.keyspace)

    def __mul__(self, other: Union[int, ChordKey]) -> ChordKey:
        other_value = other.value if type(other) == ChordKey else other
        new_value = (self.value * other_value) % self.keyspace
        return ChordKey(new_value, self.keyspace)

    def __lt__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other.value if type(other) == ChordKey else other
        return self.value < other_value

    def __le__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other.value if type(other) == ChordKey else other
        return self.value <= other_value

    def __gt__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other.value if type(other) == ChordKey else other
        return self.value > other_value

    def __ge__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other.value if type(other) == ChordKey else other
        return self.value >= other_value

    def __eq__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other.value if type(other) == ChordKey else other
        return self.value == other_value

    def __neq__(self, other: Union[int, ChordKey]) -> bool:
        other_value = other.value if type(other) == ChordKey else other
        return self.value != other_value

    def __str__(self) -> str:
        return f"{self.value} (mod {self.keyspace})"

    def __repr__(self) -> str:
        return f"{self.value} (mod {self.keyspace})"
