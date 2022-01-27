from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class Netloc:
    """Network location dataclass."""

    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"
