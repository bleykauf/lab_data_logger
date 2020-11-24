"""The command-line interface ldl."""

import logging

import click
import click_log
import rpyc

from . import logger_cmds, services_cmds

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


ldl.add_command(logger_cmds.logger)
ldl.add_command(services_cmds.services)


if __name__ == "__main__":
    ldl()
