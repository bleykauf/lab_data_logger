from dataclasses import dataclass, field

from .typing import Fields, Tags


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
    tags: Tags = field(default_factory=dict)
    fields: Fields = field(default_factory=dict)
