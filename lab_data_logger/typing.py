"""Custom types."""

from typing import Union

# types allowed by influxdb
FieldValue = Union[int, float, str, bool]
Fields = dict[str, FieldValue]
Tags = dict[str, str]
