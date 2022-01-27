import logging
from configparser import ConfigParser
from importlib import import_module

import click
import click_log
import rpyc

from . import writer as writer_module
from .common import Netloc
from .recorder import RecorderService

logger = logging.getLogger("lab_data_logger")
logger.setLevel(logging.DEBUG)

console_formatter = click_log.ColorFormatter("%(message)s")
console_handler = click_log.ClickHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)

file_formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
file_handler = logging.FileHandler("ldl.log", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(file_formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


@click.group()
@click_log.simple_verbosity_option(logger)
@click.version_option()
def ldl():
    """CLI tool for using Lab Data Logger (LDL)."""
    pass


@ldl.group("recorder")
@click.argument("netloc", type=str)
@click.pass_context
def recorder(ctx, netloc: str):
    """NETLOC is the network location 'hostname:port' where the Recorder is located."""
    host, port = netloc.split(":")
    ctx.obj = Netloc(host=host, port=int(port))


@recorder.command("start")
@click.option("--writer", default="VoidWriter", help="Writer to use for recording.")
@click.option("--config", type=click.Path(exists=True), help="Configuration file path.")
@click.pass_obj
def start_recorder_and_set_writer(
    recorder_netloc: Netloc, writer: str, config: str
) -> None:

    # parse config
    config = dict(ConfigParser().read(config)[writer])  # type: ignore

    recorder = RecorderService()
    writer_class = getattr(writer_module, writer)
    recorder.set_writer(writer_class(config))

    threaded_server = rpyc.ThreadedServer(
        service=recorder,
        port=recorder_netloc.port,
        protocol_config={"allow_public_attrs": True, "allow_pickle": True},
    )
    threaded_server.start()


@recorder.command("connect")
@click.argument("netloc", type=str)
@click.option(
    "--interval", default=1.0, help="Interval in seconds to check for new data."
)
@click.option(
    "--measurement", required=True, prompt=True, help="Measurement to record."
)
@click.pass_obj
def connect_service_to_recorder(
    recorder_netloc: Netloc,
    service_netloc: str,
    interval: float,
    measurement: str,
) -> None:
    """
    Connect a DataService located at NETLOC to the Recorder under the name MEASUREMENT.

    NETLOC is a network location 'hostname:port'.
    """
    # connect to recorder
    recorder = rpyc.connect(recorder_netloc.host, recorder_netloc.port)
    # parse netloc and create Netloc object
    host, port = service_netloc.split(":")
    netloc = Netloc(host=host, port=int(port))
    # connect recorder to service
    recorder.root.connect_source(netloc, interval=interval, measurement=measurement)


@recorder.command("disconnect")
@click.argument("netloc", type=str)
@click.pass_obj
def disconnect_service_from_recorder(
    recorder_netloc: Netloc, service_netloc: str
) -> None:
    """
    Disconnect a DataService located at NETLOC from the Recorder.

    NETLOC is a network location 'hostname:port'.
    """
    # connect to recorder
    recorder = rpyc.connect(recorder_netloc.host, recorder_netloc.port)
    # parse netloc and create Netloc object
    host, port = service_netloc.split(":")
    netloc = Netloc(host=host, port=int(port))
    # connect recorder to service
    recorder.root.disconnect_source(netloc)


@ldl.group("service")
@click.argument("netloc", type=str)
@click.pass_context
def service(ctx, netloc: str):
    """NETLOC is the network location 'hostname:port' where the Service is located."""
    host, port = netloc.split(":")
    ctx.obj = Netloc(host=host, port=int(port))


@service.command("start")
@click.argument("service", type=str)
@click.option("--config", type=click.Path(exists=True), help="Configuration file path.")
@click.pass_obj
def start_service(netloc: Netloc, service: str, config: str) -> None:
    """
    Start a DataService located at NETLOC.

    NETLOC is a network location 'hostname:port'.
    """
    # parse config
    config = dict(ConfigParser().read(config)[service])  # type: ignore

    service_class = getattr(writer_module, service)
    module, cls = service.rsplit(".", 1)
    module = import_module(module)
    service_class = getattr(module, cls)

    threaded_server = rpyc.ThreadedServer(
        service=service_class(config),
        port=netloc.port,
        protocol_config={"allow_public_attrs": True, "allow_pickle": True},
    )
    threaded_server.start()


@service.command("pull")
@click.pass_obj
def pull_from_service(netloc: Netloc) -> None:
    """
    Pull data from a LabDataService located at NETLOC.
    """
    service = rpyc.connect(netloc.host, netloc.port)
    data = service.root.get_data()
    print(data)
