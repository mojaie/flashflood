#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import time

from chorus.draw.svg import SVG
from chorus.model.graphmol import Compound
from chorus import v2000writer
from chorus.util.text import decode

from tornado import web, gen
from tornado.ioloop import IOLoop
from tornado.options import define, options, parse_command_line

from flashflood import configparser as conf
from flashflood import static
from flashflood import auth
from flashflood.core.jobqueue import JobQueue
from flashflood.interface import sqlite
from flashflood.interface import xlsx
from flashflood.workflow import db
from flashflood.workflow import similaritynetwork as simnet
from flashflood.workflow import substructure as substr
from flashflood.workflow.chemprop import ChemProp
from flashflood.workflow.gls import GLS
from flashflood.workflow.profile import Profile
from flashflood.workflow.responsetask import ResponseTask
from flashflood.workflow.rdkitfmcs import RDKitFMCS
from flashflood.workflow.rdkitmorgan import RDKitMorgan
from flashflood.workflow.sdfparser import SDFParser


class BaseHandler(web.RequestHandler):
    pass
    # def initialize(self):
    #    self.created = time.strftime("%X %x %Z")

    # def get_current_user(self):
    #     return self.get_secure_cookie("user")


class SchemaHandler(BaseHandler):
    @auth.basic_auth
    def get(self):
        """Responds with resource schema JSON

        :statuscode 200: no error
        """
        resources = []
        for r in conf.RESOURCES:
            # Server-side resource information
            rsrc = r.copy()
            rsrc.pop("resourceFile", None)
            rsrc.pop("resourceType", None)
            rsrc.pop("resourceURL", None)
            rsrc.pop("table", None)
            resources.append(rsrc)
        self.write({
            "resources": resources,
            "templates": conf.TEMPLATES,
            "compoundIDPlaceholder": conf.COMPID_PLACEHOLDER
        })


class WorkflowHandler(BaseHandler):
    @auth.basic_auth
    @gen.coroutine
    def get(self):
        """Runs calculation job and immediately responds"""
        query = json.loads(self.get_argument("query"))
        workflows = {
            "search": db.DBSearch,
            "filter": db.DBFilter,
            "chemsearch": db.ChemDBSearch,
            "chemfilter": db.ChemDBFilter,
            "exact": substr.ExactStruct,
            "profile": Profile
        }
        wf = workflows[query["type"]](query)
        task = ResponseTask(wf)
        yield task.execute()
        self.write(task.response())


class AsyncWorkflowHandler(BaseHandler):
    def initialize(self, jq, instance):
        super().initialize()
        self.jq = jq

    @auth.basic_auth
    @gen.coroutine
    def get(self):
        """Submits calculation job"""
        query = json.loads(self.get_argument("query"))
        workflows = {
            "substr": substr.Substruct,
            "supstr": substr.Superstruct,
            "chemprop": ChemProp,
            "gls": GLS,
            "rdfmcs": RDKitFMCS,
            "rdmorgan": RDKitMorgan
        }
        wf = workflows[query["type"]](query)
        task = ResponseTask(wf)
        yield self.jq.put(task)
        self.write(task.response())


class ResultHandler(BaseHandler):
    def initialize(self, jq, instance):
        super().initialize()
        self.jq = jq

    @auth.basic_auth
    @gen.coroutine
    def get(self):
        """Fetch calculation results"""
        query = json.loads(self.get_argument("query"))
        try:
            task = self.jq.get(query["id"])
        except ValueError:
            self.write({
                "id": query["id"],
                "status": "failure",
                "message": "job not found"
            })
        else:
            if query["command"] == "abort":
                self.jq.abort(query["id"])
            while 1:
                if task.status in ("done", "aborted"):
                    break
                yield gen.sleep(0.5)
            self.write(task.response())


class SimilarityNetworkHandler(BaseHandler):
    def initialize(self, jq, instance):
        super().initialize()
        self.jq = jq

    @gen.coroutine
    def post(self):
        """Submit similarity network calculation job"""
        js = json.loads(self.request.files['contents'][0]['body'].decode())
        params = json.loads(self.get_argument("params"))
        workflows = {
            "gls": simnet.GLSNetwork,
            "rdmorgan": simnet.RDKitMorganNetwork,
            "rdfmcs": simnet.RDKitFMCSNetwork,
        }
        wf = workflows[params["measure"]](js, params)
        task = ResponseTask(wf)
        yield self.jq.put(task)
        self.write(task.response())


class StructurePreviewHandler(BaseHandler):
    @auth.basic_auth
    def get(self):
        """Structure image preview"""
        query = json.loads(self.get_argument("query"))
        try:
            qmol = sqlite.query_mol(query)
        except TypeError:
            response = '<span class="msg_warn">Format Error</span>'
        except ValueError:
            response = '<span class="msg_warn">Not found</span>'
        else:
            response = SVG(qmol).contents()
        self.write(response)


class SDFileParserHandler(BaseHandler):
    @gen.coroutine
    def post(self):
        """Responds with datatable JSON made of query SDFile"""
        filename = self.request.files['contents'][0]['filename']
        contents = decode(self.request.files['contents'][0]['body'])
        params = json.loads(self.get_argument("params"))
        query = {
            "sourceFile": filename,
            "params": params
        }
        task = ResponseTask(SDFParser(contents, query))
        yield task.execute()
        self.write(task.response())


class SDFileExportHandler(BaseHandler):
    def post(self):
        js = json.loads(self.request.files['contents'][0]['body'].decode())
        cols = [c["key"] for c in js["fields"]
                if c["visible"] and c["format"] in (
                    "text", "numeric", "d3_format", "compound_id")]
        mols = []
        for rcd in js["records"]:
            mol = Compound(json.loads(rcd["__moljson"]))
            for col in cols:
                mol.data[col] = rcd[col]
            mols.append(mol)
        text = v2000writer.mols_to_text(mols)
        self.set_header("Content-Type", 'text/plain; charset="utf-8"')
        self.write(text)


class ExcelExportHandler(BaseHandler):
    def post(self):
        js = json.loads(self.request.files['contents'][0]['body'].decode())
        data = {"tables": [js]}
        buf = xlsx.json_to_xlsx(data)
        self.set_header("Content-Type", 'application/vnd.openxmlformats-office\
                        document.spreadsheetml.sheet; charset="utf-8"')
        self.write(buf.getvalue())


class ServerStatusHandler(BaseHandler):
    def initialize(self, jq, instance):
        super().initialize()
        self.jq = jq
        self.instance = instance

    @auth.basic_auth
    def get(self):
        js = {
            "totalTasks": len(self.jq),
            "queuedTasks": self.jq.queue.qsize(),
            "instance": self.instance,
            "processors": static.PROCESSES,
            "debugMode": options.debug,
            "rdkit": static.RDK_AVAILABLE,
            "numericModule": static.NUMERIC_MODULE,
            "calc": {
                "fields": [
                    {"key": "id", "format": "text"},
                    {"key": "size", "d3_format": ".3s"},
                    {"key": "status", "format": "text"},
                    {"key": "created", "format": "date"},
                    {"key": "expires", "format": "date"}
                ],
                "records": []
            }
        }
        for task, expires in self.jq.tasks_iter():
            js["calc"]["records"].append({
                "id": task.id,
                "size": task.size(),
                "status": task.status,
                "created": time.strftime(
                    "%X %x %Z", time.localtime(task.creation_time)),
                "expires": time.strftime(
                    "%X %x %Z", time.localtime(expires)),
            })
        self.write(js)


def run():
    define("port", default=8888, help="run on the given port", type=int)
    define("debug", default=False, help="run in debug mode")
    parse_command_line()
    instance_prefix = conf.INSTANCE_PREFIX
    timestamp = time.strftime("%X %x %Z", time.localtime(time.time()))
    store = {
        "jq": JobQueue(),
        "instance": "".join((instance_prefix, timestamp))
    }
    wpath = {True: conf.WEB_BUILD, False: conf.WEB_DIST}[options.debug]
    app = web.Application(
        [
            (r"/schema", SchemaHandler),
            (r"/run", WorkflowHandler),
            (r"/async", AsyncWorkflowHandler, store),
            (r"/res", ResultHandler, store),
            (r"/simnet", SimilarityNetworkHandler, store),
            (r"/strprev", StructurePreviewHandler),
            (r"/sdfin", SDFileParserHandler),
            (r"/sdfout", SDFileExportHandler),
            (r"/xlsx", ExcelExportHandler),
            (r"/server", ServerStatusHandler, store),
            (r'/(.*)', web.StaticFileHandler, {"path": wpath})
        ],
        debug=options.debug,
        compress_response=True,
        cookie_secret="_TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE_"
    )
    for ext in conf.EXTERNALS:
        mod = __import__(ext, fromlist=["handler"])
        mod.handler.install(app)
    app.listen(options.port)
    try:
        print("Server started")
        IOLoop.current().start()
    except KeyboardInterrupt:
        print("Server stopped")
