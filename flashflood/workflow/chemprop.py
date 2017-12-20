#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import operator
import re

from flashflood import static

from flashflood.core.concurrent import ConcurrentFilter
from flashflood.core.container import Container, Counter
from flashflood.core.workflow import Workflow
from flashflood.interface import sqlite
from flashflood.node.chem.descriptor import AsyncMolDescriptor
from flashflood.node.chem.molecule import AsyncMoleculeToJSON, UnpickleMolecule
from flashflood.node.field.number import AsyncNumber
from flashflood.node.monitor.count import AsyncCountRows
from flashflood.node.reader.sqlite import SQLiteReader
from flashflood.node.writer.container import ContainerWriter


def like_operator(a, b):
    """ regexp implementation of sqlite LIKE operator """
    return re.match(b.replace("%", ".*?").replace("_", "[\w ]"), a) is not None


OPERATORS = {
    "eq": operator.eq, "gt": operator.gt, "lt": operator.lt,
    "ge": operator.ge, "le": operator.le,
    "lk": like_operator, "in": lambda a, b: a in b
}


def prop_filter(func, op, val, row):
    try:
        valid = op(func(row["__molobj"]), val)
    except TypeError as e:
        print(e, row["compound_id"], val)
    else:
        if valid:
            return row


class ChemProp(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "nodes"
        desc = static.MOL_DESCS[query["key"]]
        if desc.format_type == "d3_format" or desc.format == "numeric":
            v = float(query["value"])
        else:
            v = query["value"]
        func = functools.partial(
            prop_filter, desc.function,
            OPERATORS[query["operator"]], v
        )
        self.append(SQLiteReader(
            [sqlite.find_resource(t) for t in query["targets"]],
            fields=sqlite.merged_fields(query["targets"]),
            counter=self.input_size
        ))
        self.append(UnpickleMolecule())
        self.append(ConcurrentFilter(
            func=func, residue_counter=self.done_count))
        self.append(AsyncMolDescriptor(static.MOL_DESC_KEYS))
        self.append(AsyncMoleculeToJSON())
        self.append(AsyncNumber("index", fields=[static.INDEX_FIELD]))
        self.append(AsyncCountRows(self.done_count))
        self.append(ContainerWriter(self.results))
