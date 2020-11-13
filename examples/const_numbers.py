"""A pretty minimal example of a custom LabDataService implemenation."""

from lab_data_logger.services import LabDataService


class ConstNumberService(LabDataService):
    """Service for producing two constant numbers."""

    def __init__(self):
        super(ConstNumberService, self).__init__()

    def exposed_get_data(self):
        """Get two of the best numbers there are."""
        a_number = 3
        another_number = 2
        data = {
            "fields": {"a_number": a_number, "another_number": another_number},
        }
        return data
