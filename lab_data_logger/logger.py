"""Classes and functions related to the Logger part of LDL."""

from time import sleep

import rpyc
from influxdb import InfluxDBClient
from multiprocess import Process, Queue, Value  # pylint: disable=no-name-in-module

rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


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
        Name of the measurement. This name will be used as the measurement when writing
        to an InfluxDB (default "test").
    interval : float
        Logging interval in seconds.
    """

    def __init__(self, queue, host, port, measurement, interval):
        self.queue = queue
        self.host = host
        self.port = port
        self.measurement = measurement
        self.interval = interval
        # shared value for communicating the processes status
        self._shared_counter = Value("i", 0)

    def start_process(self):
        """Start process continously pulling data and writing it to the queue."""

        self.pull_process = Process(
            target=self._pull, args=(self.queue, self._shared_counter)
        )
        self.pull_process.start()

    @property
    def counter(self):
        """
        Number of times the process has pulled data from the DataService.
        """  # noqa D401
        return self._shared_counter.value

    def _pull(self, queue, shared_counter):
        # only used inside a multiprocessing.Process
        try:
            service = rpyc.connect(self.host, self.port)
            print(
                "Connected to {} on port {}".format(
                    service.root.get_service_name(), self.port
                )
            )
        except ConnectionRefusedError as error:
            raise ConnectionRefusedError(
                "Connection to service at {}:{} refused".format(self.host, self.port)
            ) from error
        else:
            while True:
                data = service.root.exposed_get_data()
                data["measurement"] = self.measurement
                queue.put([data])
                shared_counter.value += 1
                sleep(self.interval)


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
        self.influxdb_client = InfluxDBClient(host, port, user, password, database)

        # shared value for communicating the processes status
        self._shared_counter = Value("i", 0)

    def start_process(self):
        """Start process continously reading the queue and writing to the InfluxDB."""

        self.push_process = Process(
            target=self._push, args=(self.queue, self._shared_counter)
        )
        self.push_process.start()

    @property
    def counter(self):
        """
        Number of times the process has pushed data to the InfluxDB.
        """  # noqa D401
        return self._shared_counter.value

    def _push(self, queue, shared_counter):
        while True:
            data = queue.get()
            self.influxdb_client.write_points(data)
            shared_counter.value += 1


class Logger(rpyc.Service):
    """
    Service comprised of a Pusher and a number of Pullers and methods for managing them.

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

    def __init__(self, host, port, user, password, database):
        super(Logger, self).__init__()
        self.queue = Queue()
        self.start_pusher_process(host, port, user, password, database)
        self.exposed_pullers = []

    def start_pusher_process(self, host, port, user, password, database):
        """
        Start the Pusher process.

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
        self.pusher = Pusher(self.queue, host, port, user, password, database)
        self.pusher.start_process()

    def exposed_start_puller_process(self, host, port, measurement, interval):
        """Start a Puller process."""
        try:
            puller = Puller(self.queue, host, port, measurement, interval)
            puller.start_process()
        except ConnectionRefusedError as error:
            print("jfioajfeajfojiaojfiaojfioajifoajifoa")
        else:
            self.exposed_pullers.append(puller)

    def exposed_get_display_text(self):
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
        for puller in self.exposed_pullers:
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
    print("Started logger on port {}.".format(logger_port))


def add_puller_to_logger(logger_port, netloc, measurement, interval):
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
    """
    try:
        logger = rpyc.connect("localhost", logger_port)
    except ConnectionRefusedError as error:
        raise ConnectionRefusedError(
            "Connection to Logger refused."
            "Make sure there a Logger is running on port {}.".format(logger_port),
        ) from error
    else:
        host, port = _parse_netloc(netloc)
        logger.root.exposed_start_puller_process(host, port, measurement, interval)


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
        sleep(0.5)


def _parse_netloc(netloc):
    # Split network location pair hostname:port into
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
