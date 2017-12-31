#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import time

from chorus import v2000writer
from chorus.draw.svg import SVG
from chorus.model.graphmol import Compound
from chorus.util.text import decode
from tornado import web, gen
from tornado.options import options

from flashflood import configparser as conf
from flashflood import static
from flashflood.handler import auth
from flashflood.interface import sqlite
from flashflood.interface import xlsx
from flashflood.workflow import chemprop
from flashflood.workflow import db
from flashflood.workflow import gls
from flashflood.workflow import profile
from flashflood.workflow import similaritynetwork
from flashflood.workflow import substructure
from flashflood.workflow.responsetask import ResponseTask
from flashflood.workflow import rdkitfmcs
from flashflood.workflow import rdkitmorgan
from flashflood.workflow.sdfparser import SDFParser


class WorkflowHandler(web.RequestHandler):
    def initialize(self, workflow):
        super().initialize()
        self.workflow = workflow

    @auth.basic_auth
    @gen.coroutine
    def get(self):
        """Runs calculation job and immediately responds"""
        query = json.loads(self.get_argument("query"))
        task = ResponseTask(self.workflow(query))
        yield task.execute()
        self.write(task.response())


class ChemSearch(WorkflowHandler):
    def initialize(self):
        super().initialize(db.ChemDBSearch)


class ChemFilter(WorkflowHandler):
    def initialize(self):
        super().initialize(db.ChemDBFilter)


class Profile(WorkflowHandler):
    def initialize(self):
        super().initialize(profile.Profile)


class ExactStruct(WorkflowHandler):
    def initialize(self):
        super().initialize(substructure.ExactStruct)


class AsyncWorkflowHandler(web.RequestHandler):
    def initialize(self, workflow, jobqueue, instance):
        super().initialize()
        self.workflow = workflow
        self.jobqueue = jobqueue

    @auth.basic_auth
    @gen.coroutine
    def get(self):
        """Submits calculation job"""
        query = json.loads(self.get_argument("query"))
        task = ResponseTask(self.workflow(query))
        yield self.jobqueue.put(task)
        self.write(task.response())


class Substruct(AsyncWorkflowHandler):
    def initialize(self, **kwargs):
        super().initialize(substructure.Substruct, **kwargs)


class Superstruct(AsyncWorkflowHandler):
    def initialize(self, **kwargs):
        super().initialize(substructure.Superstruct, **kwargs)


class ChemProp(AsyncWorkflowHandler):
    def initialize(self, **kwargs):
        super().initialize(chemprop.ChemProp, **kwargs)


class GLS(AsyncWorkflowHandler):
    def initialize(self, **kwargs):
        super().initialize(gls.GLS, **kwargs)


class RDKitMorgan(AsyncWorkflowHandler):
    def initialize(self, **kwargs):
        super().initialize(rdkitmorgan.RDKitMorgan, **kwargs)


class RDKitFMCS(AsyncWorkflowHandler):
    def initialize(self, **kwargs):
        super().initialize(rdkitfmcs.RDKitFMCS, **kwargs)


class SimilarityNetworkHandler(web.RequestHandler):
    def initialize(self, workflow, jobqueue, instance):
        super().initialize()
        self.workflow = workflow
        self.jobqueue = jobqueue

    @gen.coroutine
    def post(self):
        """Submit similarity network calculation job"""
        js = json.loads(self.request.files['contents'][0]['body'].decode())
        params = json.loads(self.get_argument("params"))
        task = ResponseTask(self.workflow(js, params))
        yield self.jobqueue.put(task)
        self.write(task.response())


class GLSNetwork(SimilarityNetworkHandler):
    def initialize(self, **kwargs):
        super().initialize(similaritynetwork.GLSNetwork, **kwargs)


class RDKitMorganNetwork(SimilarityNetworkHandler):
    def initialize(self, **kwargs):
        super().initialize(similaritynetwork.RDKitMorganNetwork, **kwargs)


class RDKitFMCSNetwork(SimilarityNetworkHandler):
    def initialize(self, **kwargs):
        super().initialize(similaritynetwork.RDKitFMCSNetwork, **kwargs)


class SDFileParser(web.RequestHandler):
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


class WorkflowProgress(web.RequestHandler):
    def initialize(self, jobqueue, instance):
        super().initialize()
        self.jobqueue = jobqueue

    @auth.basic_auth
    @gen.coroutine
    def get(self):
        """Fetch calculation results"""
        query = json.loads(self.get_argument("query"))
        try:
            task = self.jobqueue.get(query["id"])
        except ValueError:
            self.write({
                "id": query["id"],
                "status": "failure",
                "message": "job not found"
            })
        else:
            if query["command"] == "abort":
                self.jobqueue.abort(query["id"])
            while 1:
                if task.status in ("done", "aborted"):
                    break
                yield gen.sleep(0.5)
            self.write(task.response())


class StructurePreview(web.RequestHandler):
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


class SDFileExport(web.RequestHandler):
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


class ExcelExport(web.RequestHandler):
    def post(self):
        js = json.loads(self.request.files['contents'][0]['body'].decode())
        data = {"tables": [js]}
        buf = xlsx.json_to_xlsx(data)
        self.set_header("Content-Type", 'application/vnd.openxmlformats-office\
                        document.spreadsheetml.sheet; charset="utf-8"')
        self.write(buf.getvalue())


class Schema(web.RequestHandler):
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


class ServerStatus(web.RequestHandler):
    def initialize(self, jobqueue, instance):
        super().initialize()
        self.jobqueue = jobqueue
        self.instance = instance

    @auth.basic_auth
    def get(self):
        js = {
            "totalTasks": len(self.jobqueue),
            "queuedTasks": self.jobqueue.queue.qsize(),
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
        for task, expires in self.jobqueue.tasks_iter():
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
