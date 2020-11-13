from multiprocessing import Pipe, Process, Queue  # noqa: D100
from time import sleep

import rpyc
from blessings import Terminal
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

    def __init__(
        self,
        queue,
        host="localhost",
        port=18861,
        measurement="test",
        interval=1,
    ):
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
        print("Connected to {}".format(service.root.get_service_name()))
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
        Hostname of the InfluxDB (default 'localhost').
    port : int
        Port of the InfluxDB  (default 8083).
    user : str
        Username of the InfluxDB (default None).
    password : str
        Password for the InfluxDB (default None).
    database : str
        Name of the database that should be used (default "test")
    """

    def __init__(
        self,
        queue,
        host="localhost",
        port=8083,
        user=None,
        password=None,
        database="test",
    ):
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


class Logger:
    """
    Class comprised of a Pusher and a number of Pullers and methods for managing them.

    Parameters
    ----------
    pusher_cfg : dict
        Contains the keyworded arguments for instantiating the Pusher.
    puller_cfgs : list of dict
        A list of dictionaries, containing the keyworded arguments for instantiating
        the Pullers objects.
    """

    def __init__(self, pusher_cfg, puller_cfgs=[]):

        self.queue = Queue()
        self.start_pusher_process(pusher_cfg)

        self.pullers = []
        for puller_cfg in puller_cfgs:
            self.start_puller_process(puller_cfg)

    def start_pusher_process(self, pusher_cfg):
        """Start the Pusher process."""
        self.pusher = Pusher(self.queue, **pusher_cfg)
        self.pusher.start_process()

    def start_puller_process(self, puller_cfg):
        """Start a Puller process."""
        puller = Puller(self.queue, **puller_cfg)
        puller.start_process()
        self.pullers.append(puller)

    def print_status(self):
        """Print status of connected DataServices and the InfluxDB, continously."""
        term = Terminal()
        with term.fullscreen():
            while True:
                display_text = "\nLAB DATA LOGGER\n"
                display_text += "Logging to {} on {}:{} (processed entry {}).\n".format(
                    self.pusher.database,
                    self.pusher.host,
                    self.pusher.port,
                    self.pusher.counter,
                )

                display_text += "Pulling from these services:\n"
                display_text += (
                    "MEASUREMENT   |     HOSTNAME     |    PORT    |   COUNTER   \n"
                )
                display_text += (
                    "-----------   |   ------------   |   ------   |   -------   \n"
                )
                for puller in self.pullers:
                    display_text += (
                        "{:11.11}   |   {:12.12}   |   {:6d}   |   {:7d}\n".format(
                            puller.measurement, puller.host, puller.port, puller.counter
                        )
                    )

                print(term.clear())
                print(display_text)
                sleep(0.5)
