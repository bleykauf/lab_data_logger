"""The command-line interface ldl."""

import importlib
from multiprocessing import Process
from time import sleep
import os

import click  # noqa D100
import rpyc
from rpyc.utils.server import ThreadedServer

from .logger import Logger

rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


@click.group()
@click.version_option()
def ldl():
    """CLI tool for using Lab Data Logger (LDL)."""
    pass


@ldl.group(chain=True)
@click.option(
    "--port",
    default=18860,
    help="Port the Logger service should be startet on (default 18860)",
)
@click.pass_context
def logger(ctx, port):
    ctx.obj = port  # store the port of the Logger serve


@logger.command()
@click.option(
    "--host", default="localhost", help="Hostname of the InfluxDB (default localhost)"
)
@click.option("--port", default=8083, help="Port of the InfluxDB (default 8083)")
@click.option("--user", default=None, help="Username of the InfluxDB (optional)")
@click.option("--password", default=None, help="Password of the InfluxDB (optional)")
@click.option("--database", default="test", help="Name of the database (default test")
@click.pass_obj
def start(logger_port, host, port, user, password, database):
    """Manage the logger components of LDL."""
    logger = Logger(host, port, user, password, database)
    threaded_server = ThreadedServer(logger, port=logger_port)

    proc = Process(target=threaded_server.start)
    proc.start()
    print("Started logger on port {}.".format(logger_port))


@logger.command()
@click.option("--host", default="localhost", help="Hostname of the DataService.")
@click.option("--port", default=18861, help="Port of the DataService.")
@click.option("--measurement", default="test", help="Name of the measurement.")
@click.option("--interval", default=1, help="Logging interval in seconds.")
@click.pass_obj
def add(logger_port, host, port, measurement, interval):
    """Add a DataService to the logger."""
    logger = rpyc.connect("localhost", logger_port)
    logger.root.exposed_start_puller_process(host, port, measurement, interval)


@logger.command()
@click.pass_obj
def show(logger_port):
    """Show the status of the logger."""
    logger = rpyc.connect("localhost", logger_port)
    while True:
        display_text = logger.root.exposed_get_display_text()
        print(display_text)
        sleep(0.5)


@ldl.group(chain=True)
def service():
    """Manage the services of LDL."""
    pass


@service.command()
@click.argument("service")
@click.option(
    "--port", default=18861, help="Port where the service is running (default 18861)."
)
def run(service, port):
    """
    Start SERVICE.

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
    threaded_server = ThreadedServer(service, port=port)

    proc = Process(target=threaded_server.start)
    proc.start()
    print("Started {} on port {}.".format(service_name, port))


if __name__ == "__main__":
    ldl()
