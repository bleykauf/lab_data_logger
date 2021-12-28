"""Classes and functions related to the Logger part of LDL."""

import logging
from multiprocessing import Event, Process, Queue, Value
from time import sleep
from typing import Optional, Union

import influxdb
import rpyc

from .utils import parse_netloc

debug_logger = logging.getLogger("lab_data_logger.logger")

rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True

JOIN_TIMEOUT = 1  # timeout for joining processes
LOGGER_SHOW_INTERVAL = 0.5  # update intervall for show_logger_status


class Puller:
    def __init__(
        self,
        queue: Queue,
        host: str,
        port: int,
        measurement: str,
        interval: float,
        fields: list[str] = None,
    ):
        """Class for pulling data from a DataService.

        Args:
            queue: A queue that the pulled data is written to.
        host: Hostname where the DataService can be accessed (default 'localhost').
        port: Port at which the DataService can be accessed (default 18861).
        measurement: Name of the measurement. This name will be used as the measurement
            when writing to an InfluxDB.
        interval: Logging interval in seconds.
        fields: A list of fields to be pulled from the DataService.
        """
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
        """Number of times the process has pulled data from the DataService."""
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
    def __init__(
        self,
        queue: Queue,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ):
        """Class that reads from a queue and writes its contents to an InfluxDB.

        Args:
            queue: A queue that the pulled data is written to.
            host: Hostname of the InfluxDB.
            port: Port of the InfluxDB.
            user: Username of the InfluxDB.
            password: Password for the InfluxDB.
            database: Name of the database that should be used.
        """
        self.queue = queue
        self.host = host
        self.port = port
        self.database = database
        self.influxdb_client = influxdb.InfluxDBClient(
            host, port, user, password, database
        )

        # check connection
        available_databases = self.influxdb_client.get_list_database()
        available_databases = [item["name"] for item in available_databases]
        if self.database in available_databases:
            # shared value for communicating the processes status
            self._shared_counter = Value("i", -1)

            self.push_process = Process(
                target=self._push, args=(self.queue, self._shared_counter)
            )
        else:
            raise influxdb.exceptions.InfluxDBClientError(
                f"No database named '{self.database}' found. Make sure it exists."
            )

    @property
    def counter(self) -> int:
        """Number of times the process has pushed data to the InfluxDB."""
        return self._shared_counter.value

    def _push(self, queue: Queue, shared_counter: Value):
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
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8086,
        user: str = None,
        password: str = None,
        database: str = None,
    ):
        """Service comprised of a Pusher and a number of Pullers and methods for
        managing them.

        Args:
            host:  Hostname of the InfluxDB.
            port: Port of the InfluxDB.
            user: Username of the InfluxDB.
            password:  Password for the InfluxDB.
            database: Name of the database that should be used.
        """
        super(Logger, self).__init__()
        self.queue = Queue()
        self.pusher = Pusher(self.queue, host, port, user, password, database)
        self.pusher.push_process.start()
        debug_logger.debug("Pusher process started.")
        self.exposed_pullers = {}

    def exposed_add_puller(
        self,
        host: str,
        port: str,
        measurement: str,
        interval: float,
        fields: list[str] = None,
    ) -> None:
        """Add and start a Puller process.

        Args:
            host: Hostname where the DataService can be accessed (default 'localhost').
            port: Port at which the DataService can be accessed (default 18861).
            measurement : Name of the measurement. This name will be used as the
                measurement when writing to an InfluxDB.
            interval: Logging interval in seconds.
            fields: A list of fields to be pulled from the DataService.
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

    def exposed_remove_puller(self, netloc: Union[str, int]) -> None:
        """Stop and remove a puller from the logger.

        Args:
            netloc: Network location, e.g. localhost:18861. If an int is passed,
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

    def exposed_get_display_text(self) -> str:
        """Print status of connected DataServices and the InfluxDB, continously."""
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


def start_logger(
    logger_port: int,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> None:
    """Start a Logger in a Process and expose it via a ThreadedServer.

    Args:
        logger_port: The port the Logger's methods should be exposed on.
        host: Hostname of the InfluxDB.
        port: Port of the InfluxDB
        user: Username of the InfluxDB.
        password: Password of the InfluxDB.
        database: Name of the InfluxDB database.
    """
    logger = Logger(host, port, user, password, database)
    threaded_server = rpyc.utils.server.ThreadedServer(logger, port=logger_port)

    proc = Process(target=threaded_server.start)
    proc.start()
    debug_logger.info(f"Started logger on port {logger_port}.")


def _get_logger(port: int) -> rpyc.core.protocol.Connection:
    """Get connection to a Logger object exposed on a port.

    Args:
        port: Port the Logger is exposed on

    Returns:
        Connection to the Logger.
    """
    try:
        logger = rpyc.connect("localhost", port)
    except ConnectionRefusedError as error:
        raise ConnectionRefusedError(
            "Connection to Logger refused."
            f"Make sure there a Logger is running on port {port}.",
        ) from error
    return logger


def add_puller_to_logger(
    logger_port: int,
    netloc: Union[str, int],
    measurement: str,
    interval: float,
    fields: Optional[list[str]] = None,
) -> None:
    """Add a Puller to a Logger.

    Args:
        logger_port: The port the Logger's methods are exposed on.
        netloc: Network location of the LabDataService that should be pulled. Has the
            form "hostname:port". If an integer is provided, it is the port on
            localhost.
        measurement: Name of the measurement, i.e. the measurement field of InfluxDB.
        interval: Logging interval in seconds.
        fields: List of fields that should be returned. If more fields are
            provided by the service they will be filtered.
    """
    logger = _get_logger(logger_port)
    host, port = parse_netloc(netloc)
    logger.root.exposed_add_puller(host, port, measurement, interval, fields=fields)


def remove_puller_from_logger(logger_port: int, netloc: Union[str, int]) -> None:
    """Remove a Puller from a Logger.

    Args:
    logger_port: The port the Logger's methods are exposed on.
    netloc: Network location of the LabDataService that is being pulled. Has the form
        "hostname:port".
    """
    logger = _get_logger(logger_port)
    logger.root.exposed_remove_puller(netloc)


def show_logger_status(logger_port: int) -> None:
    """Show the status of a Logger.

    Args:
    logger_port: The port the Logger's methods are exposed on.
    """
    logger = rpyc.connect("localhost", logger_port)
    while True:
        display_text = logger.root.exposed_get_display_text()
        print(display_text)
        sleep(LOGGER_SHOW_INTERVAL)
