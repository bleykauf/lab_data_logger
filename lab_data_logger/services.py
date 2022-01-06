"""
LabDataService template class and two example implemenations.

These are the objects that provide the data that we want to log.
"""

import copy
import logging
import random
from abc import ABC, abstractmethod
from typing import Any, Union

import rpyc

debug_logger = logging.getLogger("lab_data_logger.services")


FieldValue = Union[int, float, str, bool]
Fields = dict[str, FieldValue]


class LabDataService(ABC, rpyc.Service):
    def __init__(self, config: dict[str, Any] = {}) -> None:
        """
        Base class for other data services.

        Args:
            config: Optional configuration data. Is stored as an attribute for use in
            `get_data_fields`.
        """

        super(LabDataService, self).__init__()
        self.config.update(config)  # overwrite default values
        # copy is necessary to have an actual dict and not a netref
        self.config = copy.deepcopy(self.config)
        self._post_init()

    config = {}
    """Configuration options."""

    def _post_init(self) -> None:
        """Do stuff that has to be done before the data aquisition can be started."""
        pass

    def get_data(self, requested_fields: list[str] = []) -> Fields:
        """Get the data of from the service."""

        fields = self.get_data_fields(requested_fields)
        if requested_fields:
            fields = {key: fields[key] for key in fields}
        return fields

    @abstractmethod
    def get_data_fields(self, requested_fields: list[str] = []) -> Fields:
        """Base method that has to be implemented."""
        ...


class RandomNumberService(LabDataService):
    """A service that generates random numbers between 0.0 and 1.0."""

    def get_data_fields(self, requested_fields: list[str] = []) -> Fields:
        return {"random_number": random.random()}


def start_service(service: LabDataService, port: int) -> None:
    """
    Start a service in a ThreadedServer
    """
    threaded_server = rpyc.ThreadedServer(
        service=service,
        port=port,
        protocol_config={"allow_public_attrs": True, "allow_pickle": True},
    )
    threaded_server.start()
