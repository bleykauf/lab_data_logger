"""
LabDataService template class and two example implemenations.

These are the objects that provide the data that we want to log.
"""

import copy
import logging
import random
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import rpyc
from rpyc.core.protocol import DEFAULT_CONFIG

debug_logger = logging.getLogger("lab_data_logger.services")

# multiprocessing needs pickling
DEFAULT_CONFIG["allow_pickle"] = True

SHOW_INTERVAL = 0.5
JOIN_TIMEOUT = 1


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

    def get_data(self, fields: list[str] = [], add_timestamp: bool = True) -> dict:
        """Get the data of from the service.

        Args:
            fields: A list of the data fields that should be returned. All other fields
                will be removed. This list is also passed to the `get_data_fields`
                method where it can be used to already filter during data aquisition.
                Defaults to None, i.e. all fields provided are returned.
            add_timestamp: Determines whether a timestamp should be added at the time of
                data aquisition (the default). If no timestamp is present, influxdb will
                automatically create one when the data is written to the database.

        Returns:
            A dict containing the keys "fields" and optionally "time". Note that the
            "measurments" field has to still be added later.
        """
        data = {}
        if add_timestamp:
            data["time"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # pylint: disable=assignment-from-no-return
        data["fields"] = self.get_data_fields(fields=fields)
        self.filter_fields(data, fields=fields)
        return data

    def _post_init(self) -> None:
        """Do stuff that has to be done before the data aquisition can be started."""
        pass

    @abstractmethod
    def get_data_fields(self, fields: list[str] = []) -> dict:
        """A base method that has to be implemented.

        Args:
            fields: Optional list of fields that should be returned.

        Returns:
        data: A dictionary of field : value pairs.
        """
        pass

    @staticmethod
    def filter_fields(data: dict, fields: list[str] = []) -> dict:
        """Filters the data to only contain certain fields.

        Args:
            data: Has to have an entry "fields", containing a dict of "field":value
            pairs.
        fields: Contains the fields that should be kept.

        Returns:
        data: Same as fields but only with the specified fields.
        """
        if fields:
            data["fields"] = {field: data["fields"][field] for field in fields}
        return data


class RandomNumberService(LabDataService):
    """A service that generates random numbers between 0.0 and 1.0."""

    def get_data_fields(self, fields: list[str] = []) -> dict:
        return {"random_number": random.random()}
