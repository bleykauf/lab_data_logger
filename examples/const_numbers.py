"""A pretty minimal example of a custom LabDataService implemenation."""

import random

from lab_data_logger.services import LabDataService


class ConstNumberService(LabDataService):
    """Service for producing two constant numbers."""

    # the default configuration, can be overwritten, see __init__ of LabDataService
    config = {"a_number": 2, "another_number": 3}

    def prepare_data_acquisition(self):
        self.random_number = random.random()

    def get_data_fields(self, **kwargs):
        """
        Get a random number and two  configurable numbers.
        """
        data = {
            "fixed_random_number": self.random_number,
            "a_number": self.config["a_number"],
            "another_number": self.config["another_number"],
        }
        return data
