"""Recorder classes for dumping data."""

import logging
from abc import ABC, abstractmethod
from dataclasses import asdict
from multiprocessing import Process, Queue, Value

from influxdb.client import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

logger = logging.getLogger("lab_data_logger.recorder")


class Writer(ABC):
    """Class that reads data from a queue and dumps to data somewhere."""

    def __init__(self, queue: Queue) -> None:
        self.queue = queue
        self.counter = Value("i", -1)
        self.write_process = Process(target=self.write_continously)

    @abstractmethod
    def write_continously(self) -> None:
        ...


class VoidWriter(Writer):
    """Recorder that reads the queue, increments the counter and does nothing."""

    def write_continously(self) -> None:
        self.counter.value += 1  # change from -1 to 0
        while True:
            _ = self.queue.get()
            self.counter.value += 1


class PrintWriter(Writer):
    def write_continously(self) -> None:
        self.counter.value += 1
        while True:
            message = self.queue.get()
            print(message)
            self.counter.value += 1


class InfluxDBWriter(Writer):
    """Recorder that reads from a queue and writes its contents to an InfluxDB."""

    def __init__(self, queue: Queue, client: InfluxDBClient):
        super().__init__(queue)

        self.client = client
        # check connection
        available_databases = self.client.get_list_database()
        available_databases = [item["name"] for item in available_databases]
        if self.client not in available_databases:
            # FIXME: Make this a custom exception.
            raise InfluxDBClientError(
                f"No database named '{self.client}' found. Make sure it exists."
            )

    def write_continously(self) -> None:
        """Write points to the database and increment the counter."""
        self.counter.value += 1  # change from -1 to 0
        while True:
            message = self.queue.get()
            try:
                self.client.write_points(asdict(message))
                self.counter.value += 1
            except InfluxDBClientError:
                # FIXME: Change behaviour depending on which error is thrown.
                logger.exception(f"Could not write data of {message} to the database.")
