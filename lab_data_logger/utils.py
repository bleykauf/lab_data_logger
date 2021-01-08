"""Utility functions."""

import importlib
import json
import os
import sys


def parse_netloc(netloc):
    """
    Split network location pair hostname:port into the hostname and port.

    Parameters
    ----------
    netloc : str or int
        Network location, e.g. localhost:18861. If an int is passed, localhost is
        assumed.

    Returns
    -------
    host : str
    port : int
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


def import_service(service):
    """
    Import a LabDataService from a dot separated path.

    Parameters
    ----------
    service : str or LabDataService
        Dot separated path to the LabDataService. For example, you can import the
        ConstNumberService by passing 'const_numbers.ConstNumberService' from the
        examples folder.

    Returns
    -------
    LabDataService
    """
    service_name = service.split(".")[-1]
    module_name = service[: -len(service_name) - 1]
    # add working directory to PATH, to allow to importing modules from there
    sys.path.append(os.getcwd())
    module = importlib.import_module(module_name)
    service = getattr(module, service_name)
    return service


def parse_config(config):
    # load a config file as a dict or return an empty dict
    if config:
        with open(config) as config_file:
            config = json.load(config_file)
    else:
        config = {}
    return config
