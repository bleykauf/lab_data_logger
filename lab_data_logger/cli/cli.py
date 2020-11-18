"""The command-line interface ldl."""


from . import logger_cmds, services_cmds

import click
import rpyc

rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


@click.group()
@click.version_option()
def ldl():
    """CLI tool for using Lab Data Logger (LDL)."""
    pass


ldl.add_command(logger_cmds.logger)
ldl.add_command(services_cmds.services)


if __name__ == "__main__":
    ldl()
