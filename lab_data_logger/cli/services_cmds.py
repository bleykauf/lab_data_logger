"""CLI subcommands for the services."""

import json

import click

from ..services import (
    start_service,
    pull_from_service,
    start_service_manager,
    add_service_to_service_manager,
    remove_service_from_service_manager,
)


@click.group()
def services():
    """Manage the services of LDL."""
    pass


@services.command()
@click.argument("service")
@click.argument("port")
@click.option(
    "--config", type=click.Path(exists=True), help="Path to configuration file"
)
def start(service, port, config):
    """
    Start SERVICE on PORT.

    SERVICE is a dot-separated path to the DataService class that should be started,
    e.g. ldl.services.RandomNumberService).
    """
    config = _parse_config(config)
    start_service(service, port, config)


@services.command()
@click.argument("netloc")
def pull(netloc):
    """
    Pull from a service located at NETLOC.

    NETLOC is a network location hostname:port or only the port (localhost is assumed).
    """
    print(pull_from_service(netloc))


@services.group()
@click.option(
    "--port",
    default=18859,
    help="Port the Manager runs on (default 18859)",
)
@click.pass_context
def manager(ctx, port):
    """Handle services managers of LDL."""
    ctx.obj = port  # store the port of the ServiceManager


@manager.command("start")
@click.pass_obj  # pass the manager_port
def start_manager(manager_port):
    """Start a ServiceManager."""
    start_service_manager(manager_port)


@manager.command()
@click.argument("service")
@click.argument("port")
@click.option(
    "--config", type=click.Path(exists=True), help="Path to configuration file"
)
@click.pass_obj  # pass the manager_port
def add(manager_port, service, port, config):
    """Start SERVICE on PORT and add it to the ServiceManager."""
    config = _parse_config(config)
    add_service_to_service_manager(manager_port, service, port, config)


@manager.command()
@click.argument("port")
@click.pass_obj  # pass the manager_port
def remove(manager_port, port):
    """Remove the serivce running on PORT from the ServiceManager."""
    remove_service_from_service_manager(manager_port, port)


def _parse_config(config):
    # load a config file as a dict or return an empty dict
    if config:
        with open(config) as config_file:
            config = json.load(config_file)
    else:
        config = {}
    return config
