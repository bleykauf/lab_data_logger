"""CLI subcommands for the services."""

import importlib
import os
from multiprocessing import Process

import click

import rpyc

rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


@click.group(chain=True)
def services():
    """Manage the services of LDL."""
    pass


@services.command()
@click.argument("service")
@click.argument("port")
def start(service, port):
    """
    Start SERVICE on PORT.

    SERVICE is a dot-separated path to the DataService class that should be started,
    e.g. ldl.services.RandomNumberService).
    """
    service_name = service.split(".")[-1]
    module_name = service[: -len(service_name) - 1]
    try:
        print("Trying to start {} from {}".format(service_name, module_name))
        module = importlib.import_module(module_name)
    # This is a workaround for when the module that contains the service is in the
    # current directory. In this case ModuleNotFoundError is raised for some reason.
    except ModuleNotFoundError:
        print("No module {} found".format(module_name))
        path_to_file = os.path.join(os.getcwd(), *module_name.split(".")) + ".py"
        print("Looking for {} in {}".format(service_name, path_to_file))
        spec = importlib.util.spec_from_file_location(module_name, path_to_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    service = getattr(module, service_name)
    threaded_server = rpyc.utils.server.ThreadedServer(service, port=int(port))

    proc = Process(target=threaded_server.start)
    proc.start()
    print("Started {} on port {}.".format(service_name, port))
