"""Classes and functions related to the Logger part of LDL."""

import logging
from dataclasses import field
from multiprocessing import Queue

import rpyc
from rpyc.core.protocol import DEFAULT_CONFIG

from .netloc import Netloc
from .puller import Puller, Tags
from .writer import Writer

logger = logging.getLogger("lab_data_logger.recorder")

DEFAULT_CONFIG["allow_pickle"] = True

JOIN_TIMEOUT = 1  # timeout for joining processes
LOGGER_SHOW_INTERVAL = 0.5  # update intervall for show_logger_status


class RecorderService(rpyc.Service):
    def __init__(self) -> None:
        """
        Service comprised of a Writer and a number of Pullers and methods for
        managing them.
        """
        super(RecorderService, self).__init__()
        self.queue = Queue()
        self.connected_sources: dict[Netloc, Puller] = field(default_factory=dict)

    def set_writer(self, writer: Writer) -> None:
        """Set the writer of the recorder.

        Args:
            sink: The sink to be used.
        """
        self.writer = writer
        self.writer.write_process.start()
        logger.debug("Writer process started.")

    def connect_source(
        self,
        netloc: Netloc,
        interval: float,
        measurement: str,
        tags: Tags = field(default_factory=dict),
        requested_fields: list[str] = field(default_factory=list),
    ) -> None:
        """
        Connect to a Source and start pulling data.
        """
        if netloc in self.connected_sources.keys():
            logger.error(f"{netloc} is already connected.")
        else:
            puller = Puller(
                queue=self.queue,
                netloc=netloc,
                interval=interval,
                measurement=measurement,
                requested_fields=requested_fields,
                tags=tags,
            )
            logger.info(f"Starting pull process for {netloc}.")
            puller.pull_process.start()
            self.connected_sources[netloc] = puller

    def disconnect_source(self, netloc: Netloc) -> None:
        """Stop and remove a puller from the logger."""
        try:
            puller = self.connected_sources[netloc]
            # shutting down via Event
            puller.stop_event.set()
            puller.pull_process.join(JOIN_TIMEOUT)
            if puller.pull_process.is_alive():
                # if not successful, terminate
                puller.pull_process.terminate()
            logger.info(
                "Puller process for {} exited with code {}.".format(
                    netloc, puller.pull_process.exitcode
                )
            )
            del self.connected_sources[netloc]
        except KeyError:
            logger.error(f"No Puller pulling from {netloc}")
