#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from chorus import mcsdr
from chorus import molutil
from chorus import rdkit

from flashflood.core.concurrent import ConcurrentFilter
from flashflood.core.container import Container, Counter
from flashflood.core.node import FuncNode
from flashflood.core.workflow import Workflow
from flashflood.node.chem.molecule import MoleculeFromJSON
from flashflood.node.control.filter import Filter
from flashflood.node.monitor.count import AsyncCountRows
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.transform.combination import Combination
from flashflood.node.writer.container import ContainerWriter


GRAPH_FIELDS = [
    {"key": "source", "name": "source", "d3_format": "d"},
    {"key": "target", "name": "target", "d3_format": "d"},
    {"key": "weight", "name": "weight", "d3_format": ".2f"}
]


def molsize_prefilter(cutoff, rcd):
    return len(rcd["__molobj"]) <= cutoff


def gls_array(ignoreHs, diam, tree, rcd):
    arr_rcd = {"index": rcd["index"]}
    if ignoreHs:
        mol = molutil.make_Hs_implicit(rcd["__molobj"])
    else:
        mol = rcd["__molobj"]
    try:
        arr = mcsdr.comparison_array(mol, diam, tree)
    except ValueError:
        pass
    else:
        arr_rcd["array"] = arr
    return arr_rcd


def gls_prefilter(thld, pair):
    rcd1, rcd2 = pair
    if "array" not in rcd1:
        return False
    if "array" not in rcd2:
        return False
    sm, bg = sorted((rcd1["array"][1], rcd2["array"][1]))
    return sm >= bg * thld


def gls_calc(timeout, pair):
    row1, row2 = pair
    res = mcsdr.from_array(row1["array"], row2["array"], timeout)
    return {
        "source": row1["index"],
        "target": row2["index"],
        "weight": res.local_sim(),
        "timeout": res.exec_time()["total"] >= timeout
    }


def rdkit_mol(ignoreHs, rcd):
    if ignoreHs:
        mol = molutil.make_Hs_implicit(rcd["__molobj"])
    else:
        mol = rcd["__molobj"]
    return {"index": rcd["index"], "mol": mol}


def morgan_calc(radius, pair):
    row1, row2 = pair
    score = rdkit.morgan_sim(row1["mol"], row2["mol"], radius=radius)
    return {
        "source": row1["index"],
        "target": row2["index"],
        "weight": score
    }


def fmcs_calc(timeout, pair):
    row1, row2 = pair
    res = rdkit.fmcs(row1["mol"], row2["mol"], timeout=timeout)
    return {
        "source": row1["index"],
        "target": row2["index"],
        "weight": res["similarity"],
        "timeout": res["timeout"]
    }


def thld_filter(thld, row):
    return row["weight"] >= thld


class GLSNetwork(Workflow):
    def __init__(self, contents, params, **kwargs):
        super().__init__(**kwargs)
        self.query = {"params": params}
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "edges"
        ignoreHs = params["ignoreHs"]
        thld = float(params["threshold"])
        diam = int(params["diameter"])
        tree = int(params["maxTreeSize"])
        timeout = float(params["timeout"])
        self.append(IterInput(contents["records"]))
        self.append(MoleculeFromJSON())
        self.append(FuncNode(
            functools.partial(gls_array, ignoreHs, diam, tree)
        ))
        self.append(Combination(counter=self.input_size))
        self.append(Filter(functools.partial(gls_prefilter, thld)))
        self.append(ConcurrentFilter(
            functools.partial(thld_filter, thld),
            func=functools.partial(gls_calc, timeout),
            residue_counter=self.done_count, fields=GRAPH_FIELDS
        ))
        self.append(AsyncCountRows(self.done_count))
        self.append(ContainerWriter(self.results))


class RDKitMorganNetwork(Workflow):
    def __init__(self, contents, params, **kwargs):
        super().__init__(**kwargs)
        self.query = {"params": params}
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "edges"
        self.reference = {"nodes": contents["id"]}
        ignoreHs = params["ignoreHs"]
        thld = float(params["threshold"])
        self.append(IterInput(contents["records"]))
        self.append(MoleculeFromJSON())
        self.append(FuncNode(functools.partial(rdkit_mol, ignoreHs)))
        self.append(Combination(counter=self.input_size))
        # radius=2 is ECFP4 equivalent
        self.append(ConcurrentFilter(
            functools.partial(thld_filter, thld),
            func=functools.partial(morgan_calc, 2),
            residue_counter=self.done_count, fields=GRAPH_FIELDS
        ))
        self.append(AsyncCountRows(self.done_count))
        self.append(ContainerWriter(self.results))


class RDKitFMCSNetwork(Workflow):
    def __init__(self, contents, params, **kwargs):
        super().__init__(**kwargs)
        self.query = {"params": params}
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "edges"
        self.reference = {"nodes": contents["id"]}
        ignoreHs = params["ignoreHs"]
        thld = float(params["threshold"])
        timeout = int(params["timeout"])
        self.append(IterInput(contents["records"]))
        self.append(MoleculeFromJSON())
        self.append(FuncNode(functools.partial(rdkit_mol, ignoreHs)))
        self.append(Combination(counter=self.input_size))
        self.append(ConcurrentFilter(
            functools.partial(thld_filter, thld),
            func=functools.partial(fmcs_calc, timeout),
            residue_counter=self.done_count, fields=GRAPH_FIELDS
        ))
        self.append(AsyncCountRows(self.done_count))
        self.append(ContainerWriter(self.results))
