"""The Lab Data Logger LDL."""

from ._version import get_versions

from . import logger  # noqa F401
from . import services  # noqa F401

__version__ = get_versions()["version"]
del get_versions
