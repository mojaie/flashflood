#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import os
import time
import yaml

from chorus.draw.svg import SVG
from chorus.model.graphmol import Compound
from chorus import v2000writer
from chorus.util.text import decode

from tornado import web
from tornado.ioloop import IOLoop
from tornado.options import define, options, parse_command_line

from kiwiii import excelexporter
from kiwiii import loader
from kiwiii import screenerapi
from kiwiii import handlerutil as hu
from kiwiii import tablebuilder as tb
from kiwiii import tablefilter as tf
from kiwiii import tablecolumn as tc
from kiwiii import worker as wk
from kiwiii.sqliteconnection import Connection
from kiwiii.util import debug


class BaseHandler(web.RequestHandler):
    pass
    # def initialize(self):
    #    self.created = time.strftime("%X %x %Z")

    # def get_current_user(self):
    #     return self.get_secure_cookie("user")


class SchemaHandler(BaseHandler):
    @hu.basic_auth
    def post(self):
        domain = self.get_argument("domain")
        response = {
            "domain": domain,
            "resources": []
        }
        for sq in loader.db_list():
            conn = Connection(sq)
            doc = conn.document()
            if doc["domain"] != domain:
                continue
            if doc["domain"] == "chemical":
                for tbl in doc["tables"]:
                    tc.add_calc_cols(tbl)
            response["resources"].extend(doc["tables"])
        self.write(response)


class TemplatesHandler(BaseHandler):
    @hu.basic_auth
    def post(self):
        response = {
            "templates": []
        }
        for tm in loader.report_tmpl_list():
            response["templates"].append({
                "name": os.path.splitext(os.path.basename(tm))[0],
                "sourceFile": os.path.basename(tm)
            })
        self.write(response)


class DataSourceHandler(BaseHandler):
    @hu.basic_auth
    def post(self):
        filename = self.get_argument("filename")
        response = {}
        try:
            with open(filename) as f:
                response = yaml.load(f.read())
        except FileNotFoundError:
            response["enabled"] = False
        self.write(response)


class SQLQueryHandler(BaseHandler):
    @hu.basic_auth
    def post(self):
        """Search database

        :form query: JSON encoded query

        :<json string method: one of ``sql``, ``chemsql``
        :<json object targets: list of resource entity
        :<json string key: query column key
        :<json object values: list of query value
        :<json string operator: ``eq``, ``gt``, ``lt``, ``ge``, ``le``,
            ``in``, ``lk``

        :statuscode 200: no error
        """
        query = json.loads(self.get_argument("query"))
        builder = tb.TableBuilder()
        if query["method"] == "chemsql":
            if query["operator"] == "fm":
                builder.add_filter(tf.ChemFirstMatchFilter(query))
            else:
                builder.add_filter(tf.ChemFindAllFilter(query))
            builder.add_filter(tc.IndexColumn())
            builder.add_filter(tc.StructureColumn())
            builder.add_filter(tc.CalcColumnGroup())
        elif query["method"] == 'sql':
            if query["operator"] == "fm":
                builder.add_filter(tf.FirstMatchFilter(query))
            else:
                builder.add_filter(tf.FindAllFilter(query))
        self.write(builder.flush())


class ComputationHandler(BaseHandler):
    def initialize(self, wq, store):
        super().initialize()
        self.wq = wq
        self.store = store

    @hu.basic_auth
    def post(self):
        """Submit computation query

        :form query: JSON encoded query

        :<json string method: one of ``prop``, ``exact``, ``sub``
        :<json boolean flush: if true, use map function with single process
        :<json object targets:
        :<json string key:
        :<json object values:
        :<json string operator:
        :<json string format: one of ``smiles``, ``molfile``, ``dbid``
        :<json string value: query value
        :<json string querySource:
        :<json float threshold:
        :<json int diameter: MCS-DR parameter
        :<json int maxTreeSize: MCS-DR parameter
        :<json int molSizeCutoff: MCS-DR parameter

        :statuscode 200: no error
        """
        query = json.loads(self.get_argument("query"))
        mol_filter = {
            "chemcalc": tf.ChemPropFilter,
            "exact": tf.ExactStructFilter,
            "sub": tf.SubStructFilter,
            "super": tf.SuperStructFilter,
            "mcsdr": tf.McsdrFilter,
            "gls": tf.GlsFilter,
            "rdmorgan": tf.RdMorganFilter,
            "rdfmcs": tf.RdFmcsFilter
        }
        builder = tb.TableBuilder()
        builder.add_filter(mol_filter[query["method"]](query))
        builder.add_filter(tc.IndexColumn())
        builder.add_filter(tc.StructureColumn())
        builder.add_filter(tc.CalcColumnGroup())
        if query.get("flush", 0):
            self.write(builder.flush())
        else:
            builder.queue(self)


class StructurePreviewHandler(BaseHandler):
    @hu.basic_auth
    def post(self):
        """Structure image preview

        :form query: JSON encoded query

        :<json string format: one of ``smiles``, ``molfile``, ``dbid``
        :<json string value: query value
        :<json string querySource:

        :statuscode 200: no error
        """
        query = json.loads(self.get_argument("query"))
        try:
            qmol = tf.parse_chem_query(
                query["value"], query["format"],
                query.get("querySource"))
        except TypeError:
            response = '<span class="msg_warn">Format Error</span>'
        except ValueError:
            response = '<span class="msg_warn">Not found</span>'
        else:
            response = SVG(qmol).contents()
        self.write(response)


class SdfHandler(BaseHandler):
    def post(self):
        """Structure image preview

        :form contents: request file object
        :form columns: list of SDF option attribute to show
        :form implh: if 1, make hydrogens implicit
        :form recalc: if 1, recalculate 2D coordinates

        :statuscode 200: no error
        """
        filename = self.request.files['contents'][0]['filename']
        contents = decode(self.request.files['contents'][0]['body'])
        query = json.loads(self.get_argument("query"))
        builder = tb.TableBuilder()
        builder.data["query"] = {"sourceFile": filename}
        builder.add_filter(tf.SdfFilter(contents, query))
        builder.add_filter(tc.IndexColumn())
        builder.add_filter(tc.StructureColumn())
        builder.add_filter(tc.CalcColumnGroup())
        tc.add_calc_cols(builder.data)
        # builder.add_filter(tc.ChemAliaseColumn(config["default_chemlib"]))
        self.write(builder.flush())


class GraphHandler(BaseHandler):
    def initialize(self, wq, store):
        super().initialize()
        self.wq = wq
        self.store = store

    @hu.basic_auth
    def post(self):
        """Submit graph connection calculation job

        :form query: JSON encoded query

        :<json string nodeTableId: ``smiles`` | ``molfile`` | ``dbid``
        :<json object indices: list of index
        :<json object molecules: list of molecule object
        :<json string measure: ``gls`` | ``morgan``
        :<json float threshold: graph connection score threshold
        :<json boolean ignoreHs: if true, ignore explicit hydrogens
        :<json int diameter: MCS-DR parameter
        :<json int maxTreeSize: MCS-DR parameter
        :<json int molSizeCutoff: MCS-DR parameter

        :statuscode 200: no error
        """
        query = json.loads(self.get_argument("query"))
        builder = tb.TableBuilder()
        matrix_filter = {
            "gls": tf.GlsMatrixFilter,
            "rdmorgan": tf.RdMorganMatrixFilter,
            "rdfmcs": tf.RdFmcsMatrixFilter
        }
        builder.add_filter(matrix_filter[query["measure"]](query))
        builder.queue(self)


class FetchRowsHandler(BaseHandler):
    def initialize(self, wq, store):
        super().initialize()
        self.wq = wq
        self.store = store

    @hu.basic_auth
    def post(self):
        """Submit graph connection calculation job

        :form query: JSON encoded query

        :<json string id: datatable id
        :<json string command: ``fetch`` | ``abort``

        :statuscode 200: no error
        """
        query = json.loads(self.get_argument("query"))
        try:
            data = self.store.get(query["id"])
        except KeyError:
            self.write({
                "id": query["id"],
                "status": "Failure",
                "message": "Temporary table not found."
            })
        else:
            if query["command"] == "abort":
                self.wq.abort(query["id"])
            data["progress"] = round(
                data["searchDoneCount"] / data["searchCount"] * 100, 1)
            data["recordCount"] = len(data["records"])
            self.write(data)


class ReportPreviewHandler(BaseHandler):
    def post(self):
        auth_header = self.request.headers.get('Authorization')
        # TODO: auth_header is local servers one
        # auth_decoded = base64.b64decode(auth_header[6:]).decode('utf-8')
        # user, passwd = auth_decoded.split(':')
        # print(user, passwd)
        qcsid = self.get_argument("qcsid")
        tmpl_file = self.get_argument("template")
        layer_idxs = [int(i) for i in self.get_arguments("vsel")]
        qcs = screenerapi.get_qcs_info((qcsid,), auth_header)[0]
        layer_name = {y["layerIndex"]: y["name"] for y in qcs["layers"]}
        tmpl = hu.TemplateMatcher(tmpl_file, "Preview", 320)
        arrays = screenerapi.get_all_layer_values(qcsid, 0, auth_header)
        for i in layer_idxs:
            tmpl.add_array(arrays[i], layer_name[i])
        self.write(tmpl.to_json())


class ReportHandler(BaseHandler):
    def post(self):
        auth_header = self.request.headers.get('Authorization')
        qcsid = self.get_argument("qcsid")
        tmpl_file = self.get_argument("template")
        layer_idxs = [int(i) for i in self.get_arguments("vsel")]
        stat_idxs = [int(i) for i in self.get_arguments("ssel")]
        qcs = screenerapi.get_qcs_info((qcsid,), auth_header)[0]
        layer_name = {y["layerIndex"]: y["name"] for y in qcs["layers"]}
        tmpl = hu.TemplateMatcher(tmpl_file, "Results")
        for i in layer_idxs:
            array = screenerapi.get_all_plate_values(qcsid, i, auth_header)
            tmpl.add_array(array, layer_name[i])
        data = {"tables": [tmpl.to_json()]}
        for i in stat_idxs:
            stat = screenerapi.get_all_plate_stats(qcsid, i, auth_header)
            stat["name"] = layer_name[i]
            data["tables"].append(stat)
        buf = excelexporter.json_to_xlsx(data)
        self.set_header("Content-Type", 'application/vnd.openxmlformats-office\
                        document.spreadsheetml.sheet; charset="utf-8"')
        self.write(buf.getvalue())


class ExcelExportHandler(BaseHandler):
    def post(self):
        js = json.loads(self.request.files['json'][0]['body'].decode())
        data = {"tables": [js]}
        buf = excelexporter.json_to_xlsx(data)
        self.set_header("Content-Type", 'application/vnd.openxmlformats-office\
                        document.spreadsheetml.sheet; charset="utf-8"')
        self.write(buf.getvalue())


class SDFileExportHandler(BaseHandler):
    def post(self):
        MOL = tc.MolObjectColumn()
        table = json.loads(self.request.files['json'][0]['body'].decode())
        cols = [c["key"] for c in table["columns"]
                if c["visible"] and c["sort"] != "none"]
        mols = []
        for rcd in table["records"]:
            mol = Compound(json.loads(rcd[MOL.key]))
            for col in cols:
                mol.data[col] = rcd[col]
            mols.append(mol)
        text = v2000writer.mols_to_text(mols)
        self.set_header("Content-Type", 'text/plain; charset="utf-8"')
        self.write(text)


"""
class LoginHandler(BaseHandler):
    def post(self):
        user = self.get_argument("name")
        pw = self.get_argument("pass")
        if user in config["user"] and pw == config["user"][user]["password"]:
            self.set_secure_cookie("user", user)
            self.write({"authenticated": True})
        else:
            self.write({"authenticated": False})
"""


class ServerStatusHandler(BaseHandler):
    def initialize(self, wq, store):
        super().initialize()
        self.wq = wq
        self.store = store

    @hu.basic_auth
    def post(self):
        # TODO: template_version
        # TODO: resource_version
        js = {
            "totalTasks": len(self.store.container),
            "queuedTasks": len(self.wq.queued_ids),
            "processors": wk.PROCESSES,
            "rdk": tf.RDK_AVAILABLE,
            "cython": tf.CYTHON_AVAILABLE,
            "numexpr": tf.NUMEXPR_AVAILABLE,
            "calc": {
                "columns": [
                    {"key": "id"},
                    {"key": "size"},
                    {"key": "status"},
                    {"key": "responseDate"},
                    {"key": "expires"}
                ],
                "records": []
            }
        }
        for store in self.store.container:
            t = time.mktime(time.strptime(store["responseDate"], "%X %x %Z"))
            e = t + self.store.max_age
            expires = time.strftime("%X %x %Z", time.localtime(e))
            js["calc"]["records"].append({
                "id": store["id"],
                "size": debug.total_size_str(store),
                "status": store["status"],
                "responseDate": store["responseDate"],
                "expires": expires
            })
        self.write(js)


def run():
    define("port", default=8888, help="run on the given port", type=int)
    define("debug", default=False, help="run in debug mode")
    parse_command_line()
    store = {
        "wq": wk.WorkerQueue(),
        "store": hu.TemporaryDataStore()
    }
    app = web.Application(
        [
            (r"/schema", SchemaHandler),
            (r"/templates", TemplatesHandler),
            (r"/source", DataSourceHandler),
            (r"/sql", SQLQueryHandler),
            (r"/compute", ComputationHandler, store),
            (r"/graph", GraphHandler, store),
            (r"/sdf", SdfHandler),
            (r"/xlsx", ExcelExportHandler),
            (r"/exportsdf", SDFileExportHandler),
            (r"/rows", FetchRowsHandler, store),
            (r"/strprev", StructurePreviewHandler),
            (r"/report", ReportHandler),
            (r"/reportprev", ReportPreviewHandler),
            (r"/server", ServerStatusHandler, store),
            (r'/(.*)', web.StaticFileHandler, {"path": loader.web_home_path()})
        ],
        debug=options.debug,
        compress_response=True,
        cookie_secret="_TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE_"
    )
    app.listen(options.port)
    try:
        print("Server started")
        IOLoop.instance().start()
    except KeyboardInterrupt:
        print("Server stopped")
