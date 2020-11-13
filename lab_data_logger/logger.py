"""The command-line interface ldl."""

from multiprocessing import Pipe, Process, Queue
from time import sleep

import rpyc
from influxdb import InfluxDBClient


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

    def start_process(self):
        """Start process continously pulling data and writing it to the queue."""
        # pipe for communicating the processes status
        self.pipe_from_child, pipe_to_parent = Pipe(duplex=False)

        self.pull_process = Process(
            target=self._pull, args=(self.queue, pipe_to_parent)
        )
        self.pull_process.start()

    @property
    def counter(self):
        """
        Number of times the process has pulled data from the DataService.
        """  # noqa D401
        if not hasattr(self, "_counter"):
            self._counter = -1
        # empty the pipe and get last status
        while self.pipe_from_child.poll():
            self._counter = self.pipe_from_child.recv()
        return self._counter

    def _pull(self, queue, pipe_to_parent):
        # only used inside a multiprocessing.Process
        counter = 0
        service = rpyc.connect(self.host, self.port)
        print(
            "Connected to {} on port {}".format(
                service.root.get_service_name(), self.port
            )
        )
        while True:
            data = service.root.exposed_get_data()
            data["measurement"] = self.measurement
            queue.put([data])
            counter += 1
            pipe_to_parent.send(counter)
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

    def start_process(self):
        """Start process continously reading the queue and writing to the InfluxDB."""
        # pipe for communicating the processes status
        self.pipe_from_child, pipe_to_parent = Pipe(duplex=False)

        self.push_process = Process(
            target=self._push, args=(self.queue, pipe_to_parent)
        )
        self.push_process.start()

    @property
    def counter(self):
        """
        Number of times the process has pushed data to the InfluxDB.
        """  # noqa D401
        if not hasattr(self, "_counter"):
            self._counter = -1
        # empty the pipe and get last status
        while self.pipe_from_child.poll():
            self._counter = self.pipe_from_child.recv()
        return self._counter

    def _push(self, queue, pipe_to_parent):
        counter = 0
        while True:
            data = queue.get()
            self.influxdb_client.write_points(data)
            counter += 1
            pipe_to_parent.send(counter)


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
        puller = Puller(self.queue, host, port, measurement, interval)
        puller.start_process()
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
        display_text += "MEASUREMENT   |     HOSTNAME     |    PORT    |   COUNTER   \n"
        display_text += "-----------   |   ------------   |   ------   |   -------   \n"
        for puller in self.exposed_pullers:
            display_text += "{:11.11}   |   {:12.12}   |   {:6d}   |   {:7d}\n".format(
                puller.measurement, puller.host, puller.port, puller.counter
            )

        return display_text
