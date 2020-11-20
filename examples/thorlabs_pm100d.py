from lab_data_logger.services import LabDataService

from ThorlabsPM100 import ThorlabsPM100, usbtmc
import pyvisa as visa  # also install pyvisa-py
import os
import platform


class ThorlabsPM100DService(LabDataService):
    """
    Read the power from a Thorlabs Optical Power Meter PM100D.
    """

    def __init__(self):
        super(ThorlabsPM100DService, self).__init__()

        os.system("cls" if os.name == "nt" else "clear")

        the_os = platform.system()
        if the_os == "Linux":
            inst = usbtmc.USBTMC()
        elif the_os == "Windows":
            rm = visa.ResourceManager()
            POWERMETER_ADDRESS = "USB0::0x1313::0x8078::P0020110::INSTR"
            inst = rm.open_resource(POWERMETER_ADDRESS, timeout=1000)

        self.instrument = ThorlabsPM100(inst=inst)
        self.instrument.sense.power.dc.range.auto = "ON"

    def exposed_get_data(self):
        power = self.instrument.read

        data = {"fields": {"power": power}}

        return data
