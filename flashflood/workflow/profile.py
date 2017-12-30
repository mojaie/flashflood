#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os

from flashflood import configparser as conf
from flashflood import static
from flashflood.core.workflow import Workflow
from flashflood.core.container import Container
from flashflood.interface import sqlite
from flashflood.node.field.number import Number
from flashflood.node.writer.container import ContainerWriter
from flashflood.node.reader.sqlite import SQLiteReaderFilter
from flashflood.node.record.merge import MergeRecords


class Profile(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.data_type = "nodes"
        sq_ids = []
        sq_rsrcs = []
        for target in query["targets"]:
            rsrc = conf.RESOURCES.find("id", target)
            if rsrc["resourceType"] == "sqlite":
                sq_ids.append(rsrc["id"])
                path = os.path.join(conf.SQLITE_BASE_DIR, rsrc["resourceFile"])
                sq_rsrcs.append((path, rsrc["table"]))
            """
            elif rsrc["resourceType"] == "screener_api":
                api.append(rsrc["resourceURL"])
            """
        sq_filter = SQLiteReaderFilter(
            sq_rsrcs,
            "compound_id", query["compound_id"], "=",
            fields=sqlite.merged_fields(sq_ids)
        )
        merge = MergeRecords()
        self.connect(sq_filter, merge)
        self.connect(merge, Number("index", fields=[static.INDEX_FIELD]))
        self.append(ContainerWriter(self.results))
