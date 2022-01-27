from random import random

from ..typing import Fields
from .base import LabDataService


class RandomNumberService(LabDataService):
    """A service that generates random numbers between 0.0 and 1.0."""

    def get_data_fields(self, requested_fields: list[str] = []) -> Fields:
        return {"random_number": random.random()}
