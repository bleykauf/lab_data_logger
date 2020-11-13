import click  # noqa D100
from .logger import Logger
from multiprocessing import Process
import rpyc
from rpyc.utils.server import ThreadedServer
import importlib

rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


@click.group()
@click.version_option()
def cli():
    """CLI tool for using Lab Data Logger (LDL)."""
    pass


@cli.group(chain=True)
@click.option(
    "--host", default="localhost", help="Hostname of the InfluxDB (default localhost)"
)
@click.option("--port", default=8083, help="Port of the InfluxDB (default 8083)")
@click.option("--user", default=None, help="Username of the InfluxDB (optional)")
@click.option("--password", default=None, help="Password of the InfluxDB (optional)")
@click.option("--database", default="test", help="Name of the database (default test")
@click.pass_context
def logger(ctx, host, port, user, password, database):
    """Manage the logger components of LDL."""
    pusher_cfg = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }
    logger = Logger(pusher_cfg)
    ctx.obj = logger


@logger.command()
@click.option("--host", default="localhost", help="Hostname of the DataService.")
@click.option("--port", default=18861, help="Port of the DataService.")
@click.option("--measurement", default="test", help="Name of the measurement.")
@click.option("--interval", default=1, help="Logging interval in seconds.")
@click.pass_obj
def pull(logger, host, port, measurement, interval):
    """Add a DataService to the logger."""
    puller_cfg = {
        "host": host,
        "port": port,
        "measurement": measurement,
        "interval": interval,
    }
    logger.start_puller_process(puller_cfg)


@logger.command()
@click.pass_obj
def show(logger):
    """Show the status of the logger."""
    logger.print_status()


@cli.group(chain=True)
def service():
    """Manage the services of LDL."""
    pass


@service.command()
@click.argument("service")
@click.option(
    "--port", default=18861, help="Port where the service is running (default 18861)."
)
def start(service, port):
    """
    Start SERVICE.

    SERVICE is a dot-separated path to the DataService class that should be started,
    e.g. ldl.services.RandomNumberService).
    """
    service_name = service.split(".")[-1]
    module_name = service[: -len(service_name) - 1]
    module = importlib.import_module(module_name)
    service = getattr(module, service_name)
    threaded_server = ThreadedServer(service, port=port)

    proc = Process(target=threaded_server.start)
    proc.start()
    print("Started {} on port {}.".format(service_name, port))


if __name__ == "__main__":
    cli()
