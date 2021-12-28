"""Utility functions."""

import importlib
import json
import os
import sys
from typing import Union


def parse_netloc(netloc: Union[str, int]) -> tuple[str, int]:
    """Split network location pair hostname:port into the hostname and port.

    Args:
        netloc: Network location, e.g. localhost:18861. If an int is passed, localhost
            is assumed.

    Returns:
        Tuple containing the hostname and port.
    """
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


def get_service_instance(
    service: Union[str, "LabDataService"],
    working_dir: str = None,
) -> "LabDataService":
    """Get a LabDataService from a dot separated path.

    Args:
        service: Dot separated path to the LabDataService. For example, you can import
        the ConstNumberService by passing 'const_numbers.ConstNumberService' from the
        examples folder.
        working_dir: Optionally pass the working directory. This is necessary if the
        ServiceManager is running in a different working directory than the CLI for
        adding services.
    """
    if isinstance(service, str):
        service_name = service.split(".")[-1]
        module_name = service[: -len(service_name) - 1]
        # add working directory to PATH, to allow to importing modules from there
        if not working_dir:
            working_dir = os.getcwd()
        sys.path.append(working_dir)
        module = importlib.import_module(module_name)
        service = getattr(module, service_name)
    return service


def parse_config(config: str) -> dict:
    """Load a config file as a dictionary.

    Args:
        config: Path to the config file.

    Returns:
        Dictionary containing the configuration.
    """
    if config:
        with open(config) as config_file:
            config = json.load(config_file)
    else:
        config = {}
    return config
