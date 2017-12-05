#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time

from flashflood import static
from flashflood.core.container import Container
from flashflood.core.workflow import Workflow
from flashflood.lod import ListOfDict


class ResponseWorkflow(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.done_count = Container()
        self.input_size = Container()
        self.reference = {}
        self.data_type = "nodes"

    def response(self):
        return {
            "id": self.id,
            "name": self.id[:8],
            "dataType": self.data_type,
            "schemaVersion": static.SCHEMA_VERSION,
            "revision": 0,
            "created": time.strftime("%X %x %Z",
                                     time.localtime(self.creation_time)),
            "status": self.status,
            "execTime": self.execution_time(),
            "query": self.query,
            "fields": self.fields,
            "records": self.results.records,
            "progress": self.progress(),
            "reference": self.reference
        }

    def on_submitted(self):
        super().on_submitted()
        # reorder fields
        # TODO: set hidden fields
        fields = ListOfDict(self.results.fields)
        fields.delete("key", "_molobj")
        idx = fields.pick("key", "_index")
        if idx:
            self.fields.add(idx)
        struct = fields.pick("key", "_structure")
        if struct:
            self.fields.add(struct)
        self.fields.merge(fields)

    def progress(self):
        if self.status == "done":
            return 100
        if self.status in ("running", "interrupted", "aborted"):
            try:
                p = self.done_count.value / self.input_size.value
            except ZeroDivisionError:
                return
            return round(p * 100, 1)
        else:
            return 0
