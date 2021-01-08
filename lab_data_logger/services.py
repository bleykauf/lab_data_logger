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
from time import sleep

import rpyc
from multiprocessing import Process  # pylint: disable=no-name-in-module

from .utils import parse_netloc

debug_logger = logging.getLogger("lab_data_logger.service")

# multiprocessing needs pickling
rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True

SHOW_INTERVAL = 0.5
JOIN_TIMEOUT = 1


class ServiceManager(rpyc.Service):
    def __init__(self):
        super(ServiceManager, self).__init__()
        self.exposed_services = {}

    def exposed_add_service(self, service, port, config={}):
        if port in self.exposed_services.keys():
            debug_logger.error(f"Port {port} is already being used.")
        else:
            service = _import_service(service)

        threaded_server = rpyc.utils.server.ThreadedServer(
            service(config), port=int(port)
        )
        proc = Process(target=threaded_server.start)
        proc.service_name = str(service)  # add service name as attribute for display
        proc.start()
        debug_logger.info(f"Started {service} on port {port}.")
        self.exposed_services[port] = proc

    def exposed_remove_service(self, port):
        try:
            proc = self.exposed_services[port]
            proc.join(JOIN_TIMEOUT)
            if proc.is_alive():
                proc.terminate()
            debug_logger.info(
                f"Service on port {port} exited with code {proc.exitcode}"
            )
        except KeyError:
            debug_logger.error(f"No service running on port {port}")

    def exposed_get_display_text(self):
        display_text = "\nLAB DATA LOGGER\n"
        display_text = "\nSERVICE MANAGER\n"

        display_text += "    PORT    |     SERVICE     \n"
        display_text += "   ------   |   -----------   |\n"
        for port, proc in self.exposed_services.items():
            display_text += "{:6d}   |   {:11.11}   |\n".format(port, proc.service_name)


def start_service_manager(manager_port):
    service_manager = ServiceManager()
    threaded_server = rpyc.utils.server.ThreadedServer(
        service_manager, port=manager_port
    )

    proc = Process(target=threaded_server.start)
    proc.start()
    debug_logger.info(f"Started service manager on port {manager_port}.")


def _get_service_manager(manager_port):
    try:
        service_manager = rpyc.connect("localhost", manager_port)
    except ConnectionRefusedError as error:
        raise ConnectionRefusedError(
            "Connection to ServiceManager refused."
            f"Make sure there a ServiceManager is running on port {manager_port}.",
        ) from error
    return service_manager


def add_service_to_service_manager(manager_port, service, port, config={}):
    service_manager = _get_service_manager(manager_port)
    service_manager.root.exposed_add_service(service, port, config)


def remove_service_from_service_manager(manager_port, port):
    service_manager = _get_service_manager(manager_port)
    service_manager.root.exposed_remove_service(port)


def show_service_manager_status(manager_port):
    service_manager = rpyc.connect("localhost", manager_port)
    while True:
        display_text = service_manager.root.exposed_get_display_text()
        print(display_text)
        sleep(SHOW_INTERVAL)


class LabDataService(rpyc.Service):
    """
    Base class for other data services.

    Parameters
    ----------
    config : dict
        Optional configuration data. Is stored as an attribute for use in
        `get_data_fields`.
    """

    def __init__(self, config={}):
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
            A list of the data fields that should be returned. All other fields will be
            removed. This list is also passed to the `get_data_fields` method where it
            can be used to already filter during data aquisition. Defaults to None, i.e.
            all fields provided are returned.
        add_timestamp : bool
            Determines whether a timestamp should be added at the time of data
            aquisition (the default). If no timestamp is present, influxdb will
            automatically create one when the data is written to the database.

        Returns
        -------
        data : dict
            A dict containing the keys "fields" and optionally "time". Note that the
            "measurments" field has to still be added later.


        Returns
        -------
        data : dict
            The only field is "random_numbers", containing a random number between 0.0
            and 1.0.
        """

        data = {}
        if add_timestamp:
            data["time"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # pylint: disable=assignment-from-no-return
        data["fields"] = self.get_data_fields()
        self.filter_fields(data, fields=fields)
        return data

    def prepare_data_acquisition(self):
        """Do stuff that has to be done before the data aquisition can be started."""
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


def _import_service(service):
    service_name = service.split(".")[-1]
    module_name = service[: -len(service_name) - 1]
    # add working directory to PATH, to allow to importing modules from there
    sys.path.append(os.getcwd())
    module = importlib.import_module(module_name)
    service = getattr(module, service_name)
    return service


def start_service(service, port, config={}):
    """
    Start a LabDataService.

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
    service = _import_service(service)
    threaded_server = rpyc.utils.server.ThreadedServer(service(config), port=int(port))
    debug_logger.info(f"Starting {service} on port {port}.")
    threaded_server.start()


def pull_from_service(netloc):
    """
    Pull data from a LabDataService.

    Parameters
    ----------
    netloc : str or int
        Network location, e.g. localhost:18861. If an int is passed, localhost is
        assumed.

    Returns
    -------
    data : dict
        The data pulled from the service.
    """
    host, port = parse_netloc(netloc)
    service = rpyc.connect(host, port)
    data = service.root.exposed_get_data()
    return data
