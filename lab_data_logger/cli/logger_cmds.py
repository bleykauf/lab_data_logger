"""CLI commands for the logger."""

import click

from multiprocessing import Process
from ..logger import Logger
import rpyc

from time import sleep


@click.group(chain=True)
@click.option(
    "--port",
    default=18860,
    help="Port the Logger service should be startet on (default 18860)",
)
@click.pass_context
def logger(ctx, port):
    """Manage the logger of LDL."""
    ctx.obj = port  # store the port of the Logger


@logger.command()
@click.option(
    "--host", default="localhost", help="Hostname of the InfluxDB (default localhost)"
)
@click.option("--port", default=8083, help="Port of the InfluxDB (default 8083)")
@click.option("--user", default=None, help="Username of the InfluxDB (optional)")
@click.option("--password", default=None, help="Password of the InfluxDB (optional)")
@click.option("--database", default="test", help="Name of the database (default test")
@click.pass_obj  # pass the logger_port
def start(logger_port, host, port, user, password, database):
    """Start the logger."""
    logger = Logger(host, port, user, password, database)
    threaded_server = rpyc.utils.server.ThreadedServer(logger, port=logger_port)

    proc = Process(target=threaded_server.start)
    proc.start()
    print("Started logger on port {}.".format(logger_port))


@logger.command()
@click.argument("netloc")
@click.argument("measurement")
@click.option("--interval", default=1, help="Logging interval in seconds.")
@click.pass_obj
def add(logger_port, netloc, measurement, interval):
    """
    Add DataService located at NETLOC to the logger under the name MEASUREMENT.

    NETLOC is a network location hostname:port or only the port (localhost is assumed).
    The data will be written to the MEASUREMENT.
    """
    logger = rpyc.connect("localhost", logger_port)
    host, port = _parse_netloc(netloc)
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


def _parse_netloc(netloc):
    # Split network location pair hostname:port into
    split_netloc = netloc.split(":")
    if len(split_netloc) == 2:
        # host:port pair
        host = split_netloc[0]
        port = int(split_netloc[1])
    elif len(split_netloc) == 1:
        # only port
        host = "localhost"
        port = int(split_netloc[0])
    else:
        raise ValueError("'{}' is not a valid location".format(netloc))
    return host, port
