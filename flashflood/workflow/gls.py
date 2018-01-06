#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from chorus import mcsdr
from chorus import molutil

from flashflood import static
from flashflood.core.concurrent import ConcurrentFilter
from flashflood.core.container import Container, Counter
from flashflood.core.node import FuncNode
from flashflood.core.workflow import Workflow
from flashflood.interface import sqlite
from flashflood.node.chem.descriptor import AsyncMolDescriptor
from flashflood.node.chem.molecule import AsyncMoleculeToJSON, UnpickleMolecule
from flashflood.node.control.filter import Filter
from flashflood.node.field.number import AsyncNumber
from flashflood.node.monitor.count import AsyncCountRows
from flashflood.node.reader.sqlite import SQLiteReader
from flashflood.node.writer.container import ContainerWriter


def molsize_prefilter(cutoff, rcd):
    return len(rcd["__molobj"]) <= cutoff


def gls_array(ignoreHs, diam, tree, rcd):
    if ignoreHs:
        mol = molutil.make_Hs_implicit(rcd["__molobj"])
    else:
        mol = rcd["__molobj"]
    try:
        arr = mcsdr.comparison_array(mol, diam, tree)
    except ValueError:
        pass
    else:
        rcd["array"] = arr
    return rcd


def gls_prefilter(thld, measure, qarr, rcd):
    if "array" not in rcd:
        return False
    sm, bg = sorted((qarr[1], rcd["array"][1]))
    if measure == "sim":
        return sm >= bg * thld
    elif measure == "edge":
        return sm >= thld


def gls_calc(qarr, rcd):
    res = mcsdr.local_sim(qarr, rcd["array"])
    rcd["local_sim"] = res["local_sim"]
    rcd["mcsdr"] = res["mcsdr_edges"]
    del rcd["array"]
    return rcd


def thld_filter(thld, measure, rcd):
    type_ = {"sim": "local_sim", "edge": "mcsdr"}
    return rcd[type_[measure]] >= thld


class GLS(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "nodes"
        measure = query["params"]["measure"]
        ignoreHs = query["params"]["ignoreHs"]
        thld = float(query["params"]["threshold"])
        diam = int(query["params"]["diameter"])
        tree = int(query["params"]["maxTreeSize"])
        cutoff = int(query["params"]["molSizeCutoff"])
        qmol = sqlite.query_mol(query["queryMol"])
        qarr = mcsdr.comparison_array(qmol, diam, tree)
        self.append(SQLiteReader(
            [sqlite.find_resource(t) for t in query["targets"]],
            fields=sqlite.merged_fields(query["targets"]),
            counter=self.input_size
        ))
        self.append(UnpickleMolecule())
        self.append(Filter(functools.partial(molsize_prefilter, cutoff)))
        self.append(FuncNode(
            functools.partial(gls_array, ignoreHs, diam, tree)))
        self.append(Filter(
            functools.partial(gls_prefilter, thld, measure, qarr)))
        self.append(ConcurrentFilter(
            functools.partial(thld_filter, thld, measure),
            func=functools.partial(gls_calc, qarr),
            residue_counter=self.done_count,
            fields=[
                {"key": "mcsdr", "name": "MCS-DR size", "d3_format": "d"},
                {"key": "local_sim", "name": "GLS", "d3_format": ".2f"}
            ]
        ))
        self.append(AsyncMolDescriptor(static.MOL_DESC_KEYS))
        self.append(AsyncMoleculeToJSON())
        self.append(AsyncNumber("index", fields=[static.INDEX_FIELD]))
        self.append(AsyncCountRows(self.done_count))
        self.append(ContainerWriter(self.results))
