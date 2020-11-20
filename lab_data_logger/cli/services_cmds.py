"""CLI subcommands for the services."""

import click
from ..services import start_service


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
    start_service(service, port)