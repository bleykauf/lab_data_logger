"""
LabDataService template class and one example implemenation.

These are the objects that provide the data that we want to log.
"""


import copy
import logging
import random
from datetime import datetime
from multiprocessing import Process
from time import sleep
from typing import Optional, Union

import rpyc

from .utils import get_service_instance, parse_netloc

debug_logger = logging.getLogger("lab_data_logger.service")

# multiprocessing needs pickling
rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True

SHOW_INTERVAL = 0.5
JOIN_TIMEOUT = 1


class ServiceManager(rpyc.Service):
    def __init__(self):
        super(ServiceManager, self).__init__()
        self.exposed_services = {}

    def exposed_add_service(
        self,
        service: Union[str, "LabDataService"],
        port: int,
        config: Optional[dict] = {},
        working_dir: Optional[str] = None,
    ) -> None:
        if port in self.exposed_services.keys():
            debug_logger.error(f"Port {port} is already being used.")
        else:
            service = get_service_instance(service, working_dir=working_dir)

            threaded_server = rpyc.utils.server.ThreadedServer(
                service(config), port=int(port)
            )
            proc = Process(target=threaded_server.start)
            proc.service_name = str(
                service
            )  # add service name as attribute for display
            proc.start()
            if proc.is_alive():
                debug_logger.info(f"Started {str(service)} on port {port}.")
            else:
                debug_logger.info(f"Failed to start {str(service)} on port {port}.")
            self.exposed_services[port] = proc

    def exposed_remove_service(self, port: int) -> None:
        try:
            proc = self.exposed_services[port]
            # FIXME: add an event for propererly stopping the process
            if proc.is_alive():
                proc.terminate()
                sleep(JOIN_TIMEOUT)
            debug_logger.info(
                f"Service on port {port} exited with code {proc.exitcode}"
            )
            del self.exposed_services[port]
        except KeyError:
            debug_logger.error(f"No service running on port {port}")

    def exposed_get_display_text(self) -> str:
        display_text = "\nLAB DATA LOGGER\n"
        display_text = "\nSERVICE MANAGER\n"

        display_text += "    PORT    |     SERVICE     \n"
        display_text += "   ------   |   -----------   |\n"
        for port, proc in self.exposed_services.items():
            display_text += "   {:6d}   |   {:11.11}   |\n".format(
                int(port), proc.service_name
            )

        return display_text


def start_service_manager(manager_port: int) -> None:
    service_manager = ServiceManager()
    threaded_server = rpyc.utils.server.ThreadedServer(
        service_manager, port=manager_port
    )

    proc = Process(target=threaded_server.start)
    proc.start()
    debug_logger.info(f"Started service manager on port {manager_port}.")


def _get_service_manager(manager_port: int) -> ServiceManager:
    try:
        # Allow public attribute to be able to pass config dict properly.
        service_manager = rpyc.connect(
            "localhost", manager_port, config={"allow_public_attrs": True}
        )
    except ConnectionRefusedError as error:
        raise ConnectionRefusedError(
            "Connection to ServiceManager refused."
            f"Make sure there a ServiceManager is running on port {manager_port}.",
        ) from error
    return service_manager


def add_service_to_service_manager(
    manager_port: int,
    service: "LabDataService",
    port: int,
    config: Optional[dict] = {},
    working_dir: Optional[str] = None,
) -> None:
    service_manager = _get_service_manager(manager_port)
    service_manager.root.exposed_add_service(
        service, port, config=config, working_dir=working_dir
    )


def remove_service_from_service_manager(manager_port: int, port: int) -> None:
    service_manager = _get_service_manager(manager_port)
    service_manager.root.exposed_remove_service(port)


def show_service_manager_status(manager_port: int) -> None:
    service_manager = rpyc.connect("localhost", manager_port)
    while True:
        display_text = service_manager.root.exposed_get_display_text()
        print(display_text)
        sleep(SHOW_INTERVAL)


class LabDataService(rpyc.Service):
    def __init__(self, config: dict = {}):
        """Base class for other data services.

        Args:
            config: Optional configuration data. Is stored as an attribute for use in
            `get_data_fields`.
        """

        super(LabDataService, self).__init__()
        self.config.update(config)  # overwrite default values
        # copy is necessary to have an actual dict and not a netref
        self.config = copy.deepcopy(self.config)
        self.prepare_data_acquisition()

    config = {}
    """Configuration options."""

    def exposed_get_data(
        self, fields: Optional[list[str]] = None, add_timestamp: bool = True
    ) -> dict:
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

    def prepare_data_acquisition(self) -> None:
        """Do stuff that has to be done before the data aquisition can be started."""
        pass

    def get_data_fields(self, fields: Optional[list[str]] = None) -> dict:
        """A base method that has to be implemented.

        Args:
            fields: Optional list of fields that should be returned.

        Returns:
        data: A dictionary of field : value pairs.

        Raises:
        """
        raise NotImplementedError

    @staticmethod
    def filter_fields(data: dict, fields: list[str]) -> dict:
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

    def get_data_fields(self, fields: Optional[list[str]] = None) -> dict:
        return {"random_number": random.random()}


def start_service(
    service: LabDataService,
    port: int,
    config: Optional[dict] = {},
    working_dir: Optional[str] = None,
) -> None:
    """Start a LabDataService.

    Args:
    service:  The service to be started.
    port : Port the get_data method is exposed on.
    config : Optional dictionary containing the configuration of the service.
    """
    service = get_service_instance(service, working_dir=working_dir)
    threaded_server = rpyc.utils.server.ThreadedServer(service(config), port=int(port))
    debug_logger.info(f"Starting {service} on port {port}.")
    threaded_server.start()


def pull_from_service(netloc: Union[str, int]) -> dict:
    """Pull data from a LabDataService.

    Args:
        netloc: Network location, e.g. localhost:18861. If an int is passed, localhost
        is assumed.

    Returns:
        data: The data pulled from the service.
    """
    host, port = parse_netloc(netloc)
    service = rpyc.connect(host, port)
    data = service.root.exposed_get_data()
    return data
