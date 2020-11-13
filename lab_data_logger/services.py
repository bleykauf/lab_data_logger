import random  # noqa D100
import rpyc

rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


class DataService(rpyc.Service):
    """Base class for other data servies."""

    def __init__(self):
        super(DataService, self).__init__()

    def exposed_get_data(self):
        """
        Exposes data to be accessed by a logger. Has to be implemented when subclassing.

        It should return a dict with "fields" being the only first level key and a dict
        as its only item. This dict is of the form {"field_name1": value1,
        "field_name2": value2, ...}, containing all the fields and values that should be
        logged.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError()


class RandomNumberService(DataService):
    """A service that generates random numbers between 0.0 and 1.0."""

    def __init__(self):
        super(RandomNumberService, self).__init__()

    def exposed_get_data(self):
        """
        Get a random number.

        Returns
        -------
        data : dict
            The only field is "random_numbers", containing a random number between 0.0
            and 1.0.
        """
        random_number = random.random()
        data = {
            "fields": {"random_number": random_number},
        }
        return data