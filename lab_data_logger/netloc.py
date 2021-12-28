"""Network location dataclass and utility functions."""

from dataclasses import dataclass

from typing import Union


@dataclass
class Netloc:
    """Network location dataclass."""

    port: int
    host: str = "localhost"


def create_netloc(netloc: Union[str, int]) -> Netloc:
    """Create a Netloc from a network location.

    Args:
        netloc: Network location, e.g. localhost:18861. If an int is passed, localhost
            is assumed.

    Returns:
        Network location dataclass.
    """
    if isinstance(netloc, int):
        return Netloc(port=netloc)
    host, port = netloc.split(":")
    return Netloc(host=host, port=int(port))
