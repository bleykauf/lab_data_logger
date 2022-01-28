from dataclasses import dataclass, field
from typing import Union

# types allowed by influxdb
FieldValue = Union[int, float, str, bool]
Fields = dict[str, FieldValue]


@dataclass(eq=True, frozen=True)
class Netloc:
    """Network location dataclass."""

    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class Message:
    """Message class for passing data between Puller and Writer."""

    measurement: str
    time: str
    tags: list[str] = field(default_factory=list)
    fields: Fields = field(default_factory=dict)
