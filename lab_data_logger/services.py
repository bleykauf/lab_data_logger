"""
LabDataService template class and one example implemenation.

These are the objects that provide the data that we want to log.
"""
import importlib
import logging
import os
import random
import sys

from datetime import datetime

import rpyc
from multiprocess import Process  # pylint: disable=no-name-in-module

debug_logger = logging.getLogger("lab_data_logger.service")

# multiprocessing needs pickling
rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


class LabDataService(rpyc.Service):
    def __init__(self, config={}):
        """
        Base class for other data servies.

        Parameters
        ----------
        config : dict
            Optional configuration data. Is stored as an attribute for use in `get_data_fields`.
        """

        super(LabDataService, self).__init__()
        self.config.update(config)  # overwrite default values
        self.prepare_data_acquisition()

    config = {}
    """Configuration options."""

    def exposed_get_data(self, fields=None, add_timestamp=True):
        """
        Get the data of from the service.

        Parameters
        ----------
        fields : list
            A list of the data fields that should be returned. All other fields will be removed.
            This list is also passed to the `get_data_fields` method where it can be used to already
            filter during data aquisition. Defaults to None, i.e. all fields provided are returned.
        add_timestamp : bool
            Determines whether a timestamp should be added at the time of data aquisition (the
            default). If no timestamp is present, influxdb will automatically create one when the
            data is written to the database.

        Returns
        -------
        data : dict
            A dict containing the keys "fields" and optionally "time". Note that the
            "measurments" field has to still be added later.


        Returns
        -------
        data : dict
            The only field is "random_numbers", containing a random number between 0.0 and 1.0.
        """

        data = {}
        if add_timestamp:
            data["time"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # pylint: disable=assignment-from-no-return
        data["fields"] = self.get_data_fields()
        self.filter_fields(data, fields=fields)
        return data

    def prepare_data_acquisition(self):
        """
        Do stuff that has to be done before the data aquisition can be started.
        """
        pass

    def get_data_fields(self, fields=None):
        """
        A base method that has to be implemented.

        Parameters
        ----------
        fields : list
            Optional list of fields that should be returned.

        Returns
        -------
        data : dict
            A dictionary of field : value pairs.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @staticmethod
    def filter_fields(data, fields):
        """
        Filters the data to only contain certain fields.

        Parameters
        ---------
        data : dict
            Has to have an entry "fields", containing a dict of "field":value
            pairs.
        fields : list
            Contains the fields that should be kept.

        Returns
        -------
        data : dict
            Same as fields but only with the specified fields.
        """
        if fields:
            data["fields"] = {field: data["fields"][field] for field in fields}
        return data


class RandomNumberService(LabDataService):
    """A service that generates random numbers between 0.0 and 1.0."""

    def get_data_fields(self, fields=None):

        return {"random_number": random.random()}


def start_service(service, port, config={}, no_process=False):
    """
    Start a LabDataService in a Process.

    Parameter
    ---------
    service : LabDataService
        The service to be started.
    port : int
        Port the get_data method is exposed on.
    config : dict
        Optional dictionary containing the configuration of the service.
    no_process : bool
        If set to True, the Service will not be started inside a process. This
        can be useful to avoid pickling errors in certain situations.
    """
    service_name = service.split(".")[-1]
    module_name = service[: -len(service_name) - 1]
    # add working directory to PATH, to allow to importing modules from there
    sys.path.append(os.getcwd())
    module = importlib.import_module(module_name)
    service = getattr(module, service_name)
    threaded_server = rpyc.utils.server.ThreadedServer(service(config), port=int(port))
    if no_process:
        threaded_server.start()
        debug_logger.info(
            "ThreadedServer was started directly, not in its own process."
        )
    else:
        proc = Process(target=threaded_server.start)
        proc.start()
    debug_logger.info("Started {} on port {}.".format(service, port))
