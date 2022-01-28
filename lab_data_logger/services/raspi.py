# type: ignore
from pipyadc import ADS1256, ADS1256_default_config
from pipyadc.ADS1256_definitions import (
    NEG_AINCOM,
    POS_AIN0,
    POS_AIN1,
    POS_AIN2,
    POS_AIN3,
)

from .base import LabDataService


class PiPyADCService(LabDataService):
    """
    Provides pressure readings from a vacuum gauge, read in via PiPyADC.


    Data Service to read a monitor voltage of a pressure sensor and convert that value
    to a pressure value.
    """

    def _post_init(self):
        POTI, LDR, EXT2, EXT3 = (
            POS_AIN0 | NEG_AINCOM,
            POS_AIN1 | NEG_AINCOM,
            POS_AIN2 | NEG_AINCOM,
            POS_AIN3 | NEG_AINCOM,
        )
        self.CH_SEQUENCE = (POTI, LDR, EXT2, EXT3)

        self.ads = ADS1256(ADS1256_default_config)
        self.ads.cal_self()

    def _get_data_fields(self, requested_fields: list[str] = []):
        raw_channels = self.ads.read_sequence(self.CH_SEQUENCE)
        voltages = [i * self.ads.v_per_digit for i in raw_channels]

        data = {
            "poti": voltages[0],
            "ldr": voltages[1],
            "ext2": voltages[2],
            "pressure_sensor": 10 ** (5 / 7 * voltages[3] - 10),
        }

        return data
