"""Recorder classes for dumping data."""

import logging
from abc import ABC, abstractmethod
from multiprocessing import Process, Queue, Value

import influxdb

logger = logging.getLogger("lab_data_logger.recorder")


class Recorder(ABC):
    """Class that reads data from a queue and dumps to data somewhere."""

    @property
    def counter(self) -> Value:
        """
        Shared counter to communicate the number of data points written to the database.
        """
        if not hasattr(self, "_counter"):
            self._counter = Value("i", -1)
        return self._counter

    @property
    def dump_process(self):
        if not hasattr(self, "_dump_process"):
            self._dump_process = Process(target=self.write_points)
        return self._dump_process

    @abstractmethod
    def write_points(self):
        pass


class VoidRecorder(Recorder):
    """Recorder that reads the queue, increments the counter and does nothing."""

    def write_points(self):
        self.counter.value += 1  # change from -1 to 0
        while True:
            _ = self.queue.get()
            self.counter.value += 1


class InfluxDBRecorder(Recorder):
    def __init__(
        self,
        queue: Queue,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ) -> None:
        """Recorder that reads from a queue and writes its contents to an InfluxDB.

        Args:
            queue: A queue that the pulled data is written to.
            host: Hostname of the InfluxDB.
            port: Port of the InfluxDB.
            user: Username of the InfluxDB.
            password: Password for the InfluxDB.
            database: Name of the InfluxDB database that should be used.

        Attributes:
            queue: A queue that data is read from.
            counter: A shared counter that is used to count the number of data points
                that have been written to the database.
            push_process: The process that is used to write to the database.
            influxdb_client: The InfluxDB client that is used to write to the database.
        """
        self.queue = queue
        self.influxdb_client = influxdb.InfluxDBClient(
            host, port, user, password, database
        )

        # check connection
        available_databases = self.influxdb_client.get_list_database()
        available_databases = [item["name"] for item in available_databases]
        if self.database not in available_databases:
            # FIXME: Make this a custom exception.
            raise influxdb.exceptions.InfluxDBClientError(
                f"No database named '{database}' found. Make sure it exists."
            )

    def write_points(self) -> None:
        """Write points to the database and increment the counter.

        Args:
            queue: A queue from which data is read.
        """
        self.counter.value += 1  # change from -1 to 0
        while True:
            data = self.queue.get()
            try:
                self.influxdb_client.write_points(data)
                self.counter.value += 1
            except influxdb.exceptions.InfluxDBClientError:
                # FIXME: Change behaviour depending on which error is thrown.
                logger.exception(f"Could not write data {data} to the database.")
