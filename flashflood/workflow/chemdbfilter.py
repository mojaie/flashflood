#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import operator
import re

from flashflood import configparser as conf
from flashflood import static

from flashflood.core.concurrent import ConcurrentFilter
from flashflood.core.container import Container, Counter
from flashflood.core.workflow import Workflow
from flashflood.interface import sqlite
from flashflood.lod import ListOfDict
from flashflood.node.chem.descriptor import MolDescriptor, AsyncMolDescriptor
from flashflood.node.chem.molecule import (
    MoleculeToJSON, AsyncMoleculeToJSON, UnpickleMolecule)
from flashflood.node.field.number import AsyncNumber
from flashflood.node.monitor.count import AsyncCountRows
from flashflood.node.reader.sqlite import SQLiteReader, SQLiteReaderFilter
from flashflood.node.record.merge import AsyncMergeRecords
from flashflood.node.writer.container import ContainerWriter


def like_operator(a, b):
    """ regexp implementation of sqlite LIKE operator """
    return re.match(b.replace("%", ".*?").replace("_", "[\w ]"), a) is not None


OPERATOR = {
    "eq": operator.eq, "gt": operator.gt, "lt": operator.lt,
    "ge": operator.ge, "le": operator.le, "lk": like_operator
}


SQLITE_OPERATOR = {
    "eq": "=", "gt": ">", "lt": "<", "ge": ">=", "le": "<=", "lk": "LIKE"
}


def prop_filter(func, op, val, row):
    try:
        res = op(func(row["__molobj"]), val)
    except TypeError as e:
        print(e, row["compound_id"], val)
        return False
    else:
        return res


class ChemDBFilter(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "nodes"
        merge = AsyncMergeRecords()
        for target in query["targets"]:
            rsrc = conf.RESOURCES.find("id", target)
            fields = ListOfDict(rsrc["fields"])
            if fields.find("key", query["key"]):
                # DB search
                reader = SQLiteReaderFilter(
                    [sqlite.find_resource(target)],
                    query["key"], query["value"],
                    SQLITE_OPERATOR[query["operator"]],
                    fields=fields,
                    counter=self.input_size
                )
                unpickle = UnpickleMolecule()
                self.connect(reader, unpickle)
                self.append(MolDescriptor(static.MOL_DESC_KEYS))
                jmol = MoleculeToJSON()
                self.append(jmol)
            elif query["key"] in static.MOL_DESCS:
                # Virtual field
                desc = static.MOL_DESCS[query["key"]]
                if desc.format_type == "d3_format" or desc.format == "numeric":
                    v = float(query["value"])
                else:
                    v = query["value"]
                reader = SQLiteReader(
                    [sqlite.find_resource(target)],
                    fields=fields,
                    counter=self.input_size
                )
                unpickle = UnpickleMolecule()
                self.connect(reader, unpickle)
                self.append(ConcurrentFilter(
                    functools.partial(prop_filter, desc.function,
                                      OPERATOR[query["operator"]], v),
                    residue_counter=self.done_count))
                self.append(AsyncMolDescriptor(static.MOL_DESC_KEYS))
                jmol = AsyncMoleculeToJSON()
                self.append(jmol)
            self.connect(jmol, merge)
        self.connect(
            merge,
            AsyncNumber("index", fields=[static.INDEX_FIELD])
        )
        self.append(AsyncCountRows(self.done_count))
        self.append(ContainerWriter(self.results))
