#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import operator
import re

from flashflood import static
from flashflood.node.chem.descriptor import AsyncMolDescriptor
from flashflood.node.chem.molecule import AsyncMoleculeToJSON, UnpickleMolecule
from flashflood.node.function.filter import MPFilter
from flashflood.node.function.number import AsyncNumber
from flashflood.node.monitor.count import CountRows, AsyncCountRows
from flashflood.node.reader.sqlite import SQLiteReader
from flashflood.node.writer.container import AsyncContainerWriter
from flashflood.workflow.responseworkflow import ResponseWorkflow


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


class ChemProp(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        desc = static.MOL_DESCS[query["key"]]
        if desc.format_type == "d3_format" or desc.format == "numeric":
            vs = [float(v) for v in query["values"]]
        else:
            vs = query["values"]
        # TODO Is "IN" query necessary?
        v = {True: vs, False: vs[0]}[query["operator"] == "in"]
        func = functools.partial(
            prop_filter, desc.function,
            OPERATORS[query["operator"]], v
        )
        self.append(SQLiteReader(query))
        self.append(CountRows(self.input_size))
        self.append(UnpickleMolecule())
        self.append(MPFilter(func, residue_counter=self.done_count))
        self.append(AsyncMolDescriptor(static.MOL_DESC_KEYS))
        self.append(AsyncMoleculeToJSON())
        self.append(AsyncNumber())
        self.append(AsyncCountRows(self.done_count))
        self.append(AsyncContainerWriter(self.results))
