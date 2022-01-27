"""Reading out Meerstetter TECs via XPort."""

from meer_tec import TEC, XPort

from .base import LabDataService


class XPortTECService(LabDataService):
    def _post_init(self):
        self.xport = XPort(self.config["host"], self.config["port"])
        self.tecs = [TEC(self.xport, i) for i in self.config["tec_addresses"]]

    def _get_data_fields(self, requested_fields: list[str] = []):
        data = {}
        for tec in self.tecs:
            for field in self.config["fields"]:
                data[f"tec_{tec.addr}_{field}"] = getattr(tec, field, "null")
        return data
