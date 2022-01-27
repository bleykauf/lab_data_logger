"""LabDataService for the Thorlabs PM 100 powermeter."""
from pymeasure.instruments.thorlabs import ThorlabsPM100USB

from .base import LabDataService


class ThorlabsPM100DService(LabDataService):
    """
    Read the power from a Thorlabs Optical Power Meter PM100D.
    """

    config = {"address": "USB0::0x1313::0x8078::P0020110::INSTR", "wavelength": 780}

    def _post_init(self):
        self.instrument = ThorlabsPM100USB(self.config["address"])

    def _get_data_fields(self):
        data = {"power": self.instrument.read.power}
        return data
