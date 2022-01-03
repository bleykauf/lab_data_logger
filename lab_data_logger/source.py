import logging
from multiprocessing import Event, Process, Queue, Value
from time import sleep
from typing import Optional, Union

import rpyc
from rpyc.core.protocol import DEFAULT_CONFIG, Connection
from rpyc.utils.server import ThreadedServer

from .targets import Target
from .utils import parse_netloc

logger = logging.getLogger("lab_data_logger.logging")


class Source:
    def __init__(
        self,
        queue: Queue,
        host: str,
        port: int,
        measurement: str,
        interval: float,
        fields: list[str] = [],
    ) -> None:
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
            logger.info(
                f"Connected to {service.root.get_service_name()} on port {self.port}."
            )

        except ConnectionRefusedError:
            logger.exception(
                f"Connection to service at {self.host}:{self.port} refused."
            )
            logger.warning(f"Stopping pull process from {self.host}:{self.port}.")
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
                    logger.error(
                        f"Connection to {self.host}:{self.port} closed by peer."
                    )
                    logger.warning(
                        f"Stopping pull process from {self.host}:{self.port}."
                    )
                    stop_event.set()
