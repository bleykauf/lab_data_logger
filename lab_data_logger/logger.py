"""Classes and functions related to the Logger part of LDL."""

import logging
from time import sleep

import influxdb
import rpyc

from multiprocessing import Event, Process, Queue, Value

from .utils import parse_netloc

debug_logger = logging.getLogger("lab_data_logger.logger")

rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True

JOIN_TIMEOUT = 1  # timeout for joining processes
LOGGER_SHOW_INTERVAL = 0.5  # update intervall for show_logger_status


class Puller:
    """
    Class for pulling data from a DataService.

    Parameters
    ----------
    queue : multiprocessing.Queue
        A queue that the pulled data is written to.
    host : str
        Hostname where the DataService can be accessed (default 'localhost').
    port : int
        Port at which the DataService can be accessed (default 18861).
    measurement : str
        Name of the measurement. This name will be used as the measurement when
        writing to an InfluxDB (default "test").
    interval : float
        Logging interval in seconds.
    """

    def __init__(self, queue, host, port, measurement, interval, fields=None):
        self.queue = queue
        self.host = host
        self.port = port
        self.measurement = measurement
        self.interval = interval
        self.fields = fields
        # shared value for communicating the processes status
        self._shared_counter = Value("i", -1)
        self.stop_event = Event()
        self.pull_process = Process(
            target=self._pull,
            args=(self.queue, self._shared_counter, self.stop_event, self.fields),
        )

    @property
    def counter(self):
        """
        Number of times the process has pulled data from the DataService.
        """  # noqa D401
        return self._shared_counter.value

    def _pull(self, queue, shared_counter, stop_event, fields=None):
        # the worker of the pulling process
        try:
            service = rpyc.connect(self.host, self.port)
            debug_logger.info(
                f"Connected to {service.root.get_service_name()} on port {self.port}."
            )

        except ConnectionRefusedError:
            debug_logger.exception(
                f"Connection to service at {self.host}:{self.port} refused."
            )
            debug_logger.warning(f"Stopping pull process from {self.host}:{self.port}.")
            stop_event.set()
        else:
            shared_counter.value += 1  # change from -1 to 0
            # worker loop
            while not stop_event.is_set():
                try:
                    data = service.root.exposed_get_data(fields=fields)
                    data["measurement"] = self.measurement
                    queue.put([data])
                    shared_counter.value += 1
                    sleep(self.interval)
                except EOFError:
                    debug_logger.error(
                        f"Connection to {self.host}:{self.port} closed by peer."
                    )
                    debug_logger.warning(
                        f"Stopping pull process from {self.host}:{self.port}."
                    )
                    stop_event.set()


class Pusher:
    """
    Class that reads from a queue and writes its contents to an InfluxDB.

    Parameters
    ----------
    queue : multiprocessing.Queue
        A queue that the pulled data is written to.
    host : str
        Hostname of the InfluxDB.
    port : int
        Port of the InfluxDB.
    user : str
        Username of the InfluxDB.
    password : str
        Password for the InfluxDB.
    database : str
        Name of the database that should be used.
    """

    def __init__(self, queue, host, port, user, password, database):
        self.queue = queue
        self.host = host
        self.port = port
        self.database = database
        self.influxdb_client = influxdb.InfluxDBClient(
            host, port, user, password, database
        )

        # shared value for communicating the processes status
        self._shared_counter = Value("i", -1)

        self.push_process = Process(
            target=self._push, args=(self.queue, self._shared_counter)
        )

    @property
    def counter(self):
        """
        Number of times the process has pushed data to the InfluxDB.
        """  # noqa D401
        return self._shared_counter.value

    def _push(self, queue, shared_counter):
        shared_counter.value += 1  # change from -1 to 0
        while True:
            data = queue.get()
            try:
                self.influxdb_client.write_points(data)
            except influxdb.exceptions.InfluxDBClientError:
                # FIXME: Change behaviour depending on which error is thrown.
                debug_logger.exception(f"Could not write data {data} to the database.")

            shared_counter.value += 1


class Logger(rpyc.Service):
    """
    Service comprised of a Pusher and a number of Pullers and methods for
    managing them.

    Parameters
    ----------
    host : str
        Hostname of the InfluxDB.
    port : int
        Port of the InfluxDB.
    user : str
        Username of the InfluxDB.
    password : str
        Password for the InfluxDB.
    database : str
        Name of the database that should be used.
    """

    def __init__(
        self, host="localhost", port=8086, user=None, password=None, database=None
    ):
        super(Logger, self).__init__()
        self.queue = Queue()
        self.pusher = Pusher(self.queue, host, port, user, password, database)
        self.pusher.push_process.start()
        debug_logger.debug("Pusher process started.")
        self.exposed_pullers = {}

    def exposed_add_puller(self, host, port, measurement, interval, fields=None):
        """
        Add and start a Puller process.

        Parameters
        ----------
        host : str
            Hostname where the DataService can be accessed (default 'localhost').
        port : int
            Port at which the DataService can be accessed (default 18861).
        measurement : str
            Name of the measurement. This name will be used as the measurement
            when writing to an InfluxDB.
        interval : float
            Logging interval in seconds.
        """
        netloc = f"{host}:{port}"
        if netloc in self.exposed_pullers.keys():
            debug_logger.error(f"{netloc} is already being pulled.")
        else:
            puller = Puller(
                self.queue, host, port, measurement, interval, fields=fields
            )
            debug_logger.info(f"Starting pull process for {netloc}.")
            puller.pull_process.start()
            self.exposed_pullers[netloc] = puller

    def exposed_remove_puller(self, netloc):
        """
        Stop and remove a puller from the logger.

        netloc : str or int
            Network location, e.g. localhost:18861. If an int is passed,
            localhost is assumed.
        """
        host, port = parse_netloc(netloc)
        netloc = f"{host}:{port}"
        try:
            puller = self.exposed_pullers[netloc]
            # shutting down via Event
            puller.stop_event.set()
            puller.pull_process.join(JOIN_TIMEOUT)
            if puller.pull_process.is_alive():
                # if not successful, terminate
                puller.pull_process.terminate()
            debug_logger.info(
                "Puller process for {} exited with code {}.".format(
                    netloc, puller.pull_process.exitcode
                )
            )
            del self.exposed_pullers[netloc]
        except KeyError:
            debug_logger.error(f"No Puller pulling from {netloc}")

    def exposed_get_display_text(self):
        """
        Print status of connected DataServices and the InfluxDB, continously.
        """
        display_text = "\nLAB DATA LOGGER\n"
        display_text += "Logging to {} on {}:{} (processed entry {}).\n".format(
            self.pusher.database,
            self.pusher.host,
            self.pusher.port,
            self.pusher.counter,
        )

        display_text += "Pulling from these services:\n"
        display_text += (
            "MEASUREMENT   |     HOSTNAME        |    PORT    |   COUNTER   \n"
        )
        display_text += (
            "-----------   |   ---------------   |   ------   |   -------   \n"
        )
        for _, puller in self.exposed_pullers.items():
            display_text += "{:11.11}   |   {:15.15}   |   {:6d}   |   {:7d}\n".format(
                puller.measurement, puller.host, puller.port, puller.counter
            )

        return display_text


def start_logger(logger_port, host, port, user, password, database):
    """
    Start a Logger in a Process and expose it via a ThreadedServer.

    Parameters
    ----------
    logger_port : int
        The port the Logger's methods should be exposed on.
    host : str
        Hostname of the InfluxDB.
    port : int
        Port of the InfluxDB
    user : str
        Username of the InfluxDB.
    password : str
        Password of the InfluxDB.
    database : str
        Name of the InfluxDB database.
    """
    logger = Logger(host, port, user, password, database)
    threaded_server = rpyc.utils.server.ThreadedServer(logger, port=logger_port)

    proc = Process(target=threaded_server.start)
    proc.start()
    debug_logger.info("Started logger on port {}.".format(logger_port))


def _get_logger(port):
    """
    Get connection to a Logger object exposed on a port.

    Parameters
    ----------
    port : int
        Port the Logger is exposed on

    Returns
    -------
    logger : rpyc.core.protocol.Connection
        Connection to the Logger.
    """
    try:
        logger = rpyc.connect("localhost", port)
    except ConnectionRefusedError as error:
        raise ConnectionRefusedError(
            "Connection to Logger refused."
            "Make sure there a Logger is running on port {}.".format(port),
        ) from error
    return logger


def add_puller_to_logger(logger_port, netloc, measurement, interval, fields=None):
    """
    Add a Puller to a Logger.

    Parameters
    ----------
    logger_port : int
        The port the Logger's methods are exposed on.
    netloc : str or int
        Network location of the LabDataService that should be pulled. Has the
        form "hostname:port". If an integer is provided, it is the port on
        localhost.
    measurment : str
        Name of the measurement, i.e. the measurement field of InfluxDB.
    interval : int
        Logging interval in seconds.
    fields : list
        Optional list of fields that should be returned. If more fields are
        provided by the service they will be filtered.
    """
    logger = _get_logger(logger_port)
    host, port = parse_netloc(netloc)
    logger.root.exposed_add_puller(host, port, measurement, interval, fields=fields)


def remove_puller_from_logger(logger_port, netloc):
    """
    Remove a Puller from a Logger.

    Parameters
    ----------
    logger_port : int
        The port the Logger's methods are exposed on.
    netloc : str
        Network location of the LabDataService that is being pulled. Has the
        form "hostname:port".
    """
    logger = _get_logger(logger_port)
    logger.root.exposed_remove_puller(netloc)


def show_logger_status(logger_port):
    """
    Show the status of a Logger.

    Paramters
    ---------
    logger_port : int
        The port the Logger's methods are exposed on.
    """
    logger = rpyc.connect("localhost", logger_port)
    while True:
        display_text = logger.root.exposed_get_display_text()
        print(display_text)
        sleep(LOGGER_SHOW_INTERVAL)
