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
from flashflood.node.monitor.count import AsyncCountRows
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.transform.combination import Combination
from flashflood.node.writer.container import ContainerWriter


GRAPH_FIELDS = [
    {"key": "source", "name": "source", "d3_format": "d"},
    {"key": "target", "name": "target", "d3_format": "d"},
    {"key": "weight", "name": "weight", "d3_format": ".2f"}
]


def gls_filter(params, pair):
    row1, row2 = pair
    thld = float(params["threshold"])
    sm, bg = sorted((row1["array"][1], row2["array"][1]))
    if sm < bg * thld:  # threshold filter
        return
    res = mcsdr.local_sim(row1["array"], row2["array"])
    if res["local_sim"] >= thld:
        row = {
            "source": row1["index"],
            "target": row2["index"],
            "weight": res["local_sim"]
        }
        return row


def morgan_filter(params, pair):
    row1, row2 = pair
    thld = float(params["threshold"])
    sim = rdkit.morgan_sim(row1["mol"], row2["mol"], radius=2)
    if sim >= thld:
        row = {
            "source": row1["index"],
            "target": row2["index"],
            "weight": sim
        }
        return row


def fmcs_filter(params, pair):
    row1, row2 = pair
    thld = float(params["threshold"])
    res = rdkit.fmcs(row1["mol"], row2["mol"], timeout=params["timeout"])
    if res["similarity"] >= thld:
        row = {
            "source": row1["index"],
            "target": row2["index"],
            "weight": res["similarity"],
            "canceled": res["canceled"]
        }
        return row


def gls_array(params, rcd):
    if params["ignoreHs"]:
        mol = molutil.make_Hs_implicit(rcd["__molobj"])
    else:
        mol = rcd["__molobj"]
    diam = int(params["diameter"])
    tree = int(params["maxTreeSize"])
    arr = mcsdr.comparison_array(mol, diam, tree)
    return {"index": rcd["index"], "array": arr}


def rdkit_mol(params, rcd):
    if params["ignoreHs"]:
        mol = molutil.make_Hs_implicit(rcd["__molobj"])
    else:
        mol = rcd["__molobj"]
    return {"index": rcd["index"], "mol": mol}


class GLSNetwork(Workflow):
    def __init__(self, contents, params, **kwargs):
        super().__init__(**kwargs)
        self.query = {"params": params}
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "edges"
        self.reference = {"nodes": contents["id"]}
        self.append(IterInput(contents["records"]))
        self.append(MoleculeFromJSON())
        self.append(FuncNode(functools.partial(gls_array, params)))
        self.append(Combination(counter=self.input_size))
        self.append(ConcurrentFilter(
            func=functools.partial(gls_filter, params),
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
        self.append(IterInput(contents["records"]))
        self.append(MoleculeFromJSON())
        self.append(FuncNode(functools.partial(rdkit_mol, params)))
        self.append(Combination(counter=self.input_size))
        self.append(ConcurrentFilter(
            func=functools.partial(morgan_filter, params),
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
        self.append(IterInput(contents["records"]))
        self.append(MoleculeFromJSON())
        self.append(FuncNode(functools.partial(rdkit_mol, params)))
        self.append(Combination(counter=self.input_size))
        self.append(ConcurrentFilter(
            func=functools.partial(fmcs_filter, params),
            residue_counter=self.done_count, fields=GRAPH_FIELDS
        ))
        self.append(AsyncCountRows(self.done_count))
        self.append(ContainerWriter(self.results))
