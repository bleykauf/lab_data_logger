"""
LabDataService template class and one example implemenation.

These are the objects that provide the data that we want to log.
"""

import importlib
import os
import random
import sys
from multiprocess import Process  # pylint: disable=no-name-in-module

import rpyc

# multiprocessing needs pickling
rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


class LabDataService(rpyc.Service):
    """Base class for other data servies."""

    def __init__(self):
        super(LabDataService, self).__init__()

    def exposed_get_data(self):
        """
        Exposes data to be accessed by a logger. Has to be implemented when subclassing.

        It should return a dict with "fields" being the only first level key and a dict
        as its only item. This dict is of the form {"field_name1": value1,
        "field_name2": value2, ...}, containing all the fields and values that should be
        logged.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError()


class RandomNumberService(LabDataService):
    """A service that generates random numbers between 0.0 and 1.0."""

    def __init__(self):
        super(RandomNumberService, self).__init__()

    def exposed_get_data(self):
        """
        Get a random number.

        Returns
        -------
        data : dict
            The only field is "random_numbers", containing a random number between 0.0
            and 1.0.
        """
        random_number = random.random()
        data = {
            "fields": {"random_number": random_number},
        }
        return data


def start_service(service, port):
    """
    Start a LabDataService in a Process.

    Parameter
    ---------
    service : LabDataService
        The service to be started.
    port : int
        Port the get_data method is exposed on.
    """
    service_name = service.split(".")[-1]
    module_name = service[: -len(service_name) - 1]
    # add working directory to PATH, to allow to importing modules from there
    sys.path.append(os.getcwd())
    module = importlib.import_module(module_name)
    service = getattr(module, service_name)
    threaded_server = rpyc.utils.server.ThreadedServer(service, port=int(port))

    proc = Process(target=threaded_server.start)
    proc.start()
    print("Started {} on port {}.".format(service, port))
