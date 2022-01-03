"""
LabDataService template class and one example implemenation.

These are the objects that provide the data that we want to log.
"""


import logging
from multiprocessing import Process
from time import sleep
from typing import Optional, Union

import rpyc
from rpyc.core.protocol import DEFAULT_CONFIG, Connection
from rpyc.utils.server import ThreadedServer

from .services import LabDataService
from .utils import get_service_instance

debug_logger = logging.getLogger("lab_data_logger.pool")

# multiprocessing needs pickling
DEFAULT_CONFIG["allow_pickle"] = True
SHOW_INTERVAL = 0.5
JOIN_TIMEOUT = 1


class ServicePool(rpyc.Service):
    def __init__(self) -> None:
        super(self.__class__, self).__init__()
        self.exposed_services = {}

    def exposed_add_service(
        self,
        service: Union[str, LabDataService],
        port: int,
        config: Optional[dict] = {},
        working_dir: Optional[str] = None,
    ) -> None:
        if port in self.exposed_services.keys():
            debug_logger.error(f"Port {port} is already being used.")
        else:
            service = get_service_instance(service, working_dir=working_dir)

            threaded_server = ThreadedServer(service(config), port=int(port))
            proc = Process(target=threaded_server.start)
            # name process for display
            proc.name = str(service)
            proc.start()
            if proc.is_alive():
                debug_logger.info(f"Started {str(service)} on port {port}.")
            else:
                debug_logger.info(f"Failed to start {str(service)} on port {port}.")
            self.exposed_services[port] = proc

    def remove_service(self, port: int) -> None:
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

    def get_display_text(self) -> str:
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
    service_manager = ServicePool()
    threaded_server = ThreadedServer(service_manager, port=manager_port)

    proc = Process(target=threaded_server.start)
    proc.start()
    debug_logger.info(f"Started service manager on port {manager_port}.")


def get_service_manager(manager_port: int) -> Connection:
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
    service: LabDataService,
    port: int,
    config: Optional[dict] = {},
    working_dir: Optional[str] = None,
) -> None:
    service_manager = get_service_manager(manager_port)
    service_manager.root.add_service(
        service, port, config=config, working_dir=working_dir
    )


def remove_service_from_service_manager(manager_port: int, port: int) -> None:
    service_manager = get_service_manager(manager_port)
    service_manager.root.remove_service(port)


def show_service_manager_status(manager_port: int) -> None:
    service_manager = rpyc.connect("localhost", manager_port)
    while True:
        display_text = service_manager.root.exposed_get_display_text()
        print(display_text)
        sleep(SHOW_INTERVAL)
