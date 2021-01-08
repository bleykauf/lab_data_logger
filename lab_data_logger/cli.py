"""The command-line interface for ldl."""

import json
import logging

import click
import click_log
import rpyc

from . import logger, services

debug_logger = logging.getLogger("lab_data_logger")
debug_logger.setLevel(logging.DEBUG)

console_formatter = click_log.ColorFormatter("%(message)s")
console_handler = click_log.ClickHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)

file_formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
file_handler = logging.FileHandler("ldl.log", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(file_formatter)

debug_logger.addHandler(console_handler)
debug_logger.addHandler(file_handler)

rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


@click.group()
@click_log.simple_verbosity_option(debug_logger)
@click.version_option()
def ldl():
    """CLI tool for using Lab Data Logger (LDL)."""
    pass


@ldl.group("logger")
@click.option(
    "--port",
    default=18860,
    help="Port the Logger service should be startet on (default 18860)",
)
@click.pass_context
def logger_cli(ctx, port):
    """Manage the logger of LDL."""
    ctx.obj = port  # store the port of the Logger


@logger_cli.command()
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
    logger.start_logger(logger_port, host, port, user, password, database)


@logger_cli.command()
@click.argument("netloc")
@click.argument("measurement")
@click.option("--interval", default=1.0, help="Logging interval in seconds.")
@click.pass_obj
def add(logger_port, netloc, measurement, interval):
    """
    Add DataService located at NETLOC to the logger under the name MEASUREMENT.

    NETLOC is a network location hostname:port or only the port (localhost is assumed).
    The data will be written to the MEASUREMENT.
    """
    # FIXME: fields is not yer configurable form the command line
    logger.add_puller_to_logger(logger_port, netloc, measurement, interval, fields=None)


@logger_cli.command()
@click.argument("netloc")
@click.pass_obj
def remove(logger_port, netloc):
    """Remove DataService located at NETLOC from the logger."""
    logger.remove_puller_from_logger(logger_port, netloc)


@logger_cli.command()
@click.pass_obj
def show(logger_port):
    """Show the status of the logger."""
    logger.show_logger_status(logger_port)


@ldl.group("services")
def services_cli():
    """Manage the services of LDL."""
    pass


@services_cli.command("start")
@click.argument("service")
@click.argument("port")
@click.option(
    "--config", type=click.Path(exists=True), help="Path to configuration file"
)
def services_start(service, port, config):
    """
    Start SERVICE on PORT.

    SERVICE is a dot-separated path to the DataService class that should be started,
    e.g. ldl.services.RandomNumberService).
    """
    config = _parse_config(config)
    services.start_service(service, port, config)


@services_cli.command("pull")
@click.argument("netloc")
def services_pull(netloc):
    """
    Pull from a service located at NETLOC.

    NETLOC is a network location hostname:port or only the port (localhost is assumed).
    """
    print(services.pull_from_service(netloc))


@services_cli.group()
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
def manager_start(manager_port):
    """Start a ServiceManager."""
    services.start_service_manager(manager_port)


@manager.command("add")
@click.argument("service")
@click.argument("port")
@click.option(
    "--config", type=click.Path(exists=True), help="Path to configuration file"
)
@click.pass_obj  # pass the manager_port
def manager_add(manager_port, service, port, config):
    """Start SERVICE on PORT and add it to the ServiceManager."""
    config = _parse_config(config)
    services.add_service_to_service_manager(manager_port, service, port, config)


@manager.command("remove")
@click.argument("port")
@click.pass_obj  # pass the manager_port
def manager_remove(manager_port, port):
    """Remove the serivce running on PORT from the ServiceManager."""
    services.remove_service_from_service_manager(manager_port, port)


def _parse_config(config):
    # load a config file as a dict or return an empty dict
    if config:
        with open(config) as config_file:
            config = json.load(config_file)
    else:
        config = {}
    return config


if __name__ == "__main__":
    ldl()
