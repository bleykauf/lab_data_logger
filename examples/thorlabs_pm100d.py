from lab_data_logger.services import LabDataService

from ThorlabsPM100 import ThorlabsPM100, usbtmc
import pyvisa as visa  # also install pyvisa-py
import os
import platform


class ThorlabsPM100DService(LabDataService):
    """
    Read the power from a Thorlabs Optical Power Meter PM100D.
    """

    config = {"address": "USB0::0x1313::0x8078::P0020110::INSTR"}

    def prepare_data_acquisition(self):

        os.system("cls" if os.name == "nt" else "clear")

        the_os = platform.system()
        if the_os == "Linux":
            inst = usbtmc.USBTMC()
        elif the_os == "Windows":
            rm = visa.ResourceManager()
            inst = rm.open_resource(self.config["address"], timeout=1000)

        self.instrument = ThorlabsPM100(inst=inst)
        self.instrument.sense.power.dc.range.auto = "ON"

    def get_data_fields(self):
        power = self.instrument.read

        data = {"power": power}

        return data
