from random import random

from ..common import Fields
from .base import LabDataService


class RandomNumberService(LabDataService):
    """A service that generates random numbers between 0.0 and 1.0."""

    def _get_data_fields(self, requested_fields: list[str] = []) -> Fields:
        return {"random_number": random()}


class ConstNumberService(LabDataService):
    """Service for producing two constant numbers."""

    # the default configuration, can be overwritten, see __init__ of LabDataService
    config = {"a_number": 2, "another_number": 3}

    def _post_init(self):
        self.random_number = random.random()

    def _get_data_fields(self, requested_fields: list[str] = []) -> Fields:
        """
        Get a random number and two  configurable numbers.
        """
        data = {
            "fixed_random_number": self.random_number,
            "a_number": self.config["a_number"],
            "another_number": self.config["another_number"],
        }
        return data
