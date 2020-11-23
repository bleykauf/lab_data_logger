"""CLI commands for the logger."""

import click

from multiprocessing import Process
from ..logger import (
    start_logger,
    add_puller_to_logger,
    remove_puller_from_logger,
    show_logger_status,
)
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
@click.option("--database", help="Name of the database", prompt="Database name: ")
@click.pass_obj  # pass the logger_port
def start(logger_port, host, port, user, password, database):
    """Start the logger."""
    start_logger(logger_port, host, port, user, password, database)


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
    add_puller_to_logger(logger_port, netloc, measurement, interval)


@logger.command()
@click.argument("netloc")
@click.pass_obj
def remove(logger_port, netloc):
    """
    Remove DataService located at NETLOC from the logger.
    """
    remove_puller_from_logger(logger_port, netloc)


@logger.command()
@click.pass_obj
def show(logger_port):
    """Show the status of the logger."""
    show_logger_status(logger_port)
