import pyvisa as visa  # also install pyvisa-py

from .base import LabDataService


class ADBoxService(LabDataService):
    """Service for producing two constant numbers."""

    def _post_init(self):
        rm = visa.ResourceManager("@py")
        self.adbox = rm.open_resource(
            self.config["address"], baud_rate=self.config["baud_rate"]
        )
        self.adbox.timeout = self.config["timeout"]

    def _get_data_fields(self, **kwargs):

        valid_data = False
        while not valid_data:
            # msg1 contains only the initial message CH *, \n and \r and sometimes some
            # of the first returned values, as work around combine strings and filter
            # afterwards
            self.adbox.write(self.config["query"])
            msg = ""
            try:
                # read buffer until empty
                while True:
                    msg += self.adbox.read()
            except visa.errors.VisaIOError:
                # if buffer is empty
                num_and_semicolon = set("0123456789;")
                msg = "".join(c for c in msg if c in num_and_semicolon)
                msg = msg.rstrip().split(";")
                msg = list(filter(None, msg))  # filter out empty substrings

            data = list(map(int, msg))  # convert str to list of int

            if len(data) == self.config["n_channels"]:
                # check data consistency, if something went wrong, return nans
                valid_data = True

        data = [data[chan] for chan in self.config["channels"]]
        data = {f: d for f, d in zip(self.config["data_fields"], data)}

        # scale the data
        for key in data.keys():
            data[key] = (data[key] - self.config["offsets"][key]) * self.config[
                "scale"
            ][key]

        # comensate backcoupling
        raw = data.copy()  # the backcoupling needs the raw, uncompensated data
        back = self.config["backcoupling"]
        for key, key2 in self.config["opposite_channel"].items():
            data[key] = (raw[key] - raw[key2] * back[key2]) / (
                1 - back[key] * back[key2]
            )

        return data
