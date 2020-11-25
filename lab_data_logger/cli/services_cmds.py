"""CLI subcommands for the services."""

import json

import click

from ..services import start_service


@click.group(chain=True)
def services():
    """Manage the services of LDL."""
    pass


@services.command()
@click.argument("service")
@click.argument("port")
@click.option("--config", type=click.Path(exists=True))
def start(service, port, config):
    """
    Start SERVICE on PORT.

    SERVICE is a dot-separated path to the DataService class that should be started,
    e.g. ldl.services.RandomNumberService).
    """
    if config:
        with open(config) as config_file:
            config = json.load(config_file)
    else:
        config = {}

    start_service(service, port, config)
