import rpyc
from lab_data_logger.netloc import Netloc
from lab_data_logger.writer import PrintWriter


def main() -> None:
    netloc = Netloc(host="localhost", port=18822)
    connection = rpyc.connect(netloc.host, netloc.port, config={"allow_pickle": True})
    service_netloc = Netloc(host="localhost", port=18813)
    connection.root.set_writer(PrintWriter)
    connection.root.connect_source(service_netloc, 1.0, "test")


if __name__ == "__main__":
    main()
