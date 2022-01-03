"""Classes and functions related to the Logger part of LDL."""

import logging
from multiprocessing import Queue

import rpyc
from rpyc.core.protocol import DEFAULT_CONFIG

from .netloc import Netloc
from .sinks import Sink
from .source import Source

logger = logging.getLogger("lab_data_logger.recorder")

DEFAULT_CONFIG["allow_pickle"] = True

JOIN_TIMEOUT = 1  # timeout for joining processes
LOGGER_SHOW_INTERVAL = 0.5  # update intervall for show_logger_status


class RecorderService(rpyc.Service):
    def __init__(self) -> None:
        """Service comprised of a Pusher and a number of Pullers and methods for
        managing them.

        Args:
            host:  Hostname of the InfluxDB.
            port: Port of the InfluxDB.
            user: Username of the InfluxDB.
            password:  Password for the InfluxDB.
            database: Name of the database that should be used.
        """
        super(RecorderService, self).__init__()
        self.queue = Queue()
        self.connected_sources: dict = {}

    def set_sink(self, sink: Sink) -> None:
        """Set the sink of the logger.

        Args:
            sink: The sink to be used.
        """
        self.sink = sink
        self.sink.write_process.start()
        logger.debug("Sink process started.")

    def connect_source(
        self,
        netloc: Netloc,
        measurement: str,
        interval: float,
        fields: list[str] = [],
    ) -> None:
        """
        Connect to a Source and start pulling

        Args:
            host: Hostname where the DataService can be accessed (default 'localhost').
            port: Port at which the DataService can be accessed (default 18861).
            measurement : Name of the measurement. This name will be used as the
                measurement when writing to an InfluxDB.
            interval: Logging interval in seconds.
            fields: A list of fields to be pulled from the DataService.
        """
        if netloc in self.connected_sources.keys():
            logger.error(f"{netloc} is already connected.")
        else:
            source = Source(
                queue=self.queue,
                netloc=netloc,
                measurement=measurement,
                interval=interval,
                fields=fields,
            )
            logger.info(f"Starting pull process for {netloc}.")
            source.pull_process.start()
            self.connected_sources[netloc] = source

    def remove_source(self, netloc: Netloc) -> None:
        """Stop and remove a puller from the logger.

        Args:
            netloc: Network location, e.g. localhost:18861. If an int is passed,
                localhost is assumed.
        """
        try:
            source = self.connected_sources[netloc]
            # shutting down via Event
            source.stop_event.set()
            source.pull_process.join(JOIN_TIMEOUT)
            if source.pull_process.is_alive():
                # if not successful, terminate
                source.pull_process.terminate()
            logger.info(
                "Puller process for {} exited with code {}.".format(
                    netloc, source.pull_process.exitcode
                )
            )
            del self.connected_sources[netloc]
        except KeyError:
            logger.error(f"No Puller pulling from {netloc}")

    @property
    def display_text(self) -> str:
        """Print status of connected DataServices and the InfluxDB, continously."""
        display_text = "\nLAB DATA LOGGER\n"
        display_text += "Logging to {} (processed entry {}).\n".format(
            self.sink,
            self.sink.counter,
        )

        display_text += "Pulling from these services:\n"
        display_text += (
            "MEASUREMENT   |     HOSTNAME        |    PORT    |   COUNTER   \n"
        )
        display_text += (
            "-----------   |   ---------------   |   ------   |   -------   \n"
        )
        for _, puller in self.connected_sources.items():
            display_text += "{:11.11}   |   {:15.15}   |   {:6d}   |   {:7d}\n".format(
                puller.measurement, puller.host, puller.port, puller.counter
            )

        return display_text
