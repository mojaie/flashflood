#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood import lod
from flashflood import sqlitehelper as helper
from flashflood import static
from flashflood.core.workflow import Workflow
from flashflood.core.container import Container
from flashflood.node.reader import sqlite
from flashflood.node.field.number import Number
from flashflood.node.writer.container import ContainerWriter
from flashflood.node.record.merge import MergeRecords


def add_rsrc_fields(fields_dict, row):
    row.update(fields_dict[row["field"]])
    del row["key"]
    return row


class Profile(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.data_type = "nodes"
        targets = lod.filtered(helper.SQLITE_RESOURCES, "domain", "activity")
        target_ids = lod.valuelist(targets, "id")
        sq = {
            "type": "filter", "targets": target_ids,
            "key": "compound_id", "operator": "eq",
            "values": (query["compound_id"],)
        }
        sq_filter = sqlite.SQLiteReaderFilter(sq)
        """
        if r["resourceType"] == "api":
            sq = {
                "type": "filter",
                "targets": resources,
                "resourceURL": r,
                "key": "id", "operator": "eq", "value": query[""]
            }
            e1, = self.add_node(httpio.HTTPResourceFilterInput(sq))
        """
        merge = MergeRecords()
        self.connect(sq_filter, merge)
        self.connect(merge, Number("index", fields=[static.INDEX_FIELD]))
        self.append(ContainerWriter(self.results))
