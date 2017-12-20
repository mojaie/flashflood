#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time

from flashflood import static
from flashflood.core.task import Task
from flashflood.lod import ListOfDict


class ResponseTask(Task):
    def response(self):
        ref = self.specs.reference if hasattr(self.specs, "reference") else {}
        return {
            "id": self.id,
            "name": self.id[:8],
            "dataType": self.specs.data_type,
            "schemaVersion": static.SCHEMA_VERSION,
            "revision": 0,
            "created": time.strftime("%X %x %Z",
                                     time.localtime(self.creation_time)),
            "status": self.status,
            "execTime": self.execution_time(),
            "query": self.specs.query,
            "fields": self.set_fields(),
            "records": self.specs.results.records,
            "progress": self.progress(),
            "reference": ref
        }

    def set_fields(self):
        fields = ListOfDict(self.specs.results.fields)
        new_fields = ListOfDict()
        fields.delete("key", "__moljson")
        fields.delete("key", "__molobj")
        idx = fields.pick("key", "index")
        if idx:
            new_fields.add(idx)
        struct = fields.pick("key", "structure")
        if struct:
            new_fields.add(struct)
        new_fields.merge(fields)
        # set hidden fields
        for f in new_fields:
            if f["key"].startswith("_"):
                f["visible"] = False
        return new_fields

    def progress(self):
        if self.status == "done":
            return 100
        if self.status in ("running", "interrupted", "aborted"):
            try:
                p = self.specs.done_count.value / self.specs.input_size.value
            except ZeroDivisionError:
                return
            return round(p * 100, 1)
        else:
            return 0
