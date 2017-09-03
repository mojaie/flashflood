
import csv
import os

from tornado import web
from contrib.screenerapi import request
from flashflood import excelexporter
from flashflood import configparser as conf


class CompoundHandler(web.RequestHandler):
    def get(self):
        """Compound results"""
        self.write("compound")


class DRCHandler(web.RequestHandler):
    def get(self):
        """DRC"""
        self.write("drc")


class ReportPreviewHandler(web.RequestHandler):
    def get(self):
        auth_header = self.request.headers.get('Authorization')
        # TODO: auth_header is local servers one
        # auth_decoded = base64.b64decode(auth_header[6:]).decode('utf-8')
        # user, passwd = auth_decoded.split(':')
        # print(user, passwd)
        qcsid = self.get_argument("qcsid")
        tmpl_file = self.get_argument("template")
        layer_idxs = [int(i) for i in self.get_arguments("vsel")]
        qcs = request.get_qcs_info((qcsid,), auth_header)[0]
        layer_name = {y["layerIndex"]: y["name"] for y in qcs["layers"]}
        tmpl = TemplateMatcher(tmpl_file, "Preview", 320)
        arrays = request.get_all_layer_values(qcsid, 0, auth_header)
        for i in layer_idxs:
            tmpl.add_array(arrays[i], layer_name[i])
        self.write(tmpl.to_json())


class ReportHandler(web.RequestHandler):
    def get(self):
        auth_header = self.request.headers.get('Authorization')
        qcsid = self.get_argument("qcsid")
        tmpl_file = self.get_argument("template")
        layer_idxs = [int(i) for i in self.get_arguments("vsel")]
        stat_idxs = [int(i) for i in self.get_arguments("ssel")]
        qcs = request.get_qcs_info((qcsid,), auth_header)[0]
        layer_name = {y["layerIndex"]: y["name"] for y in qcs["layers"]}
        tmpl = TemplateMatcher(tmpl_file, "Results")
        for i in layer_idxs:
            array = request.get_all_plate_values(qcsid, i, auth_header)
            tmpl.add_array(array, layer_name[i])
        data = {"tables": [tmpl.to_json()]}
        for i in stat_idxs:
            stat = request.get_all_plate_stats(qcsid, i, auth_header)
            stat["name"] = layer_name[i]
            data["tables"].append(stat)
        buf = excelexporter.json_to_xlsx(data)
        self.set_header("Content-Type", 'application/vnd.openxmlformats-office\
                        document.spreadsheetml.sheet; charset="utf-8"')
        self.write(buf.getvalue())


class TemplateMatcher(object):
    def __init__(self, tmpl_file, name, limit=float("inf")):
        self.name = name
        self.columns = []
        self.template = {}  # {id1: {id: id1, col: val1}, ...}
        tpath = os.path.join(conf.REPORT_TEMPLATE_DIR, tmpl_file)
        with open(tpath, newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
            for i, row in enumerate(reader):
                if i + 1 > limit:
                    break
                d = dict(zip(header, row))
                d["idx"] = i
                self.template[d[header[0]]] = d
            for h in header:
                self.columns.append({"key": h, "visible": True})

    def add_array(self, array, name):
        for k, v in array:
            if k in self.template:
                self.template[k][name] = v
        self.columns.append({"key": name, "visible": True})

    def to_json(self):
        records = sorted(self.template.values(), key=lambda x: x["idx"])
        return {
            "name": self.name,
            "columns": self.columns,
            "records": records
        }


def install(application):
    application.add_handlers(r".*", [
        (r"/screener/drc", DRCHandler),
        (r"/screener/compound", CompoundHandler),
        (r"/screener/report", ReportHandler),
        (r"/screener/reportprev", ReportPreviewHandler)
    ])
