#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood import lod
from flashflood import sqlitehelper as helper
from flashflood.core.workflow import Workflow
from flashflood.node.io import sqlite
from flashflood.node.function.number import Number
from flashflood.node.io.json import JSONResponse
from flashflood.node.record.merge import MergeRecords


def add_rsrc_fields(fields_dict, row):
    row.update(fields_dict[row["_field"]])
    del row["key"]
    return row


class Profile(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        targets = lod.filtered(helper.SQLITE_RESOURCES, "domain", "activity")
        target_ids = lod.valuelist(targets, "id")
        sq = {
            "type": "filter", "targets": target_ids,
            "key": "compound_id", "operator": "eq",
            "values": (query["compoundID"],)
        }
        sq_filter = sqlite.SQLiteFilterInput(sq)
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
        number = Number()
        response = JSONResponse(self)
        self.connect(merge, number)
        self.connect(number, response)
