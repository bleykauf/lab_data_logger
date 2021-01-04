"""Utility functions."""


def parse_netloc(netloc):
    """
    Split network location pair hostname:port into the hostname and port.

    Parameters
    ----------
    netloc : str or int
        Network location, e.g. localhost:18861. If an int is passed, localhost is
        assumed.

    Returns
    -------
    host : str
    port : int
    """
    split_netloc = netloc.split(":")
    if len(split_netloc) == 2:
        # host:port pair
        host = split_netloc[0]
        port = int(split_netloc[1])
    elif len(split_netloc) == 1:
        # only port
        host = "localhost"
        port = int(split_netloc[0])
    else:
        raise ValueError("'{}' is not a valid location".format(netloc))
    return host, port
