import logging
from multiprocessing import Event, Process, Queue, Value, _EventType
from multiprocessing.sharedctypes import Synchronized
from time import sleep

import rpyc

from .netloc import Netloc

logger = logging.getLogger("lab_data_logger.logging")


class Source:
    def __init__(
        self,
        queue: Queue,
        netloc: Netloc,
        measurement: str,
        interval: float,
        fields: list[str] = [],
    ) -> None:
        """
        Class for pulling data from a DataService and writing it to the queue.

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
        self.netloc = netloc
        self.measurement = measurement
        self.interval = interval
        self.fields = fields
        self.stop_event = Event()
        self.pull_process = Process(
            target=self._pull,
            args=(
                self.queue,
                self.counter,
                self.stop_event,
                self.netloc,
                self.measurement,
                self.interval,
                self.fields,
            ),
        )

    @property
    def counter(self) -> Synchronized:
        """Number of times the process has pulled data from the DataService."""
        if not hasattr(self, "_counter"):
            self._counter = Value("i", -1)
        return self._counter

    @staticmethod
    def _pull(
        queue: Queue,
        counter: Synchronized,
        stop_event: _EventType,
        netloc: Netloc,
        measurement: str,
        interval: float,
        fields: list[str] = [],
    ) -> None:
        # the worker of the pulling process
        try:
            service = rpyc.connect(netloc.host, netloc.port)
            logger.info(f"Connected to {service.root.get_service_name()} at {netloc}.")

        except ConnectionRefusedError:
            logger.exception(f"Connection to service at {netloc} refused.")
            logger.warning(f"Stopping pull process from {netloc}.")
            stop_event.set()
        else:
            counter.value += 1  # change from -1 to 0
            # worker loop
            while not stop_event.is_set():
                try:
                    data = service.root.get_data(fields=fields)
                    # FIXME: It is not the responsibility of this function to add the
                    # measurement name to the data.
                    data["measurement"] = measurement
                    # FIXME: Use dedicated dataclass to put on the queue.
                    queue.put([data])
                    counter.value += 1
                    sleep(interval)
                except EOFError:
                    logger.error(f"Connection to {netloc} closed by peer.")
                    logger.warning(f"Stopping pull process from {netloc}.")
                    stop_event.set()
