import logging
from dataclasses import dataclass, field
from datetime import datetime
from multiprocessing import Event, Process, Queue, Value
from time import sleep

import rpyc

from .common import Message, Netloc

logger = logging.getLogger("lab_data_logger.puller")


@dataclass
class Puller:
    """Class for pulling data from a DataService and writing it to the queue."""

    queue: Queue
    netloc: Netloc
    interval: float
    measurement: str
    tags: list[str] = field(default_factory=list)
    requested_fields: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.counter = Value("i", -1)
        self.stop_event = Event()
        self.pull_process = Process(target=self.pull_continously)

    def pull_continously(self) -> None:
        # the worker of the pulling process
        try:
            # FIXME: Use with statement
            connection = rpyc.connect(self.netloc.host, self.netloc.port)
            logger.info(f"Connected to {self.netloc}.")
        except ConnectionRefusedError:
            logger.exception(f"Connection to service at {self.netloc} refused.")
        else:
            self.counter.value += 1  # change from -1 to 0
            # worker loop
            while not self.stop_event.is_set():
                try:
                    message = Message(
                        measurement=self.measurement,
                        time=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        tags=self.tags,
                        fields=connection.root.get_data(self.requested_fields),
                    )
                    self.queue.put(message)
                    self.counter.value += 1
                    sleep(self.interval)
                except EOFError:
                    # FIXME: properly pass EOFError to the main process.
                    logger.error(
                        f"Connection to {self.netloc} closed by peer (EOFError)."
                    )
                    break
        finally:
            logger.debug("Setting stop event for pulling process.")
            self.stop_event.set()
            logger.info(f"Pulling process stopped after {self.counter.value} pulls.")
