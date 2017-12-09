#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from chorus import mcsdr
from chorus import molutil
from chorus import rdkit

from flashflood.core.node import FunctionNode
from flashflood.node.chem.molecule import MoleculeFromJSON
from flashflood.node.function.filter import MPFilter
from flashflood.node.monitor.count import CountRows, AsyncCountRows
from flashflood.node.reader.iterator import IteratorInput
from flashflood.node.transform.combination import Combination
from flashflood.node.writer.container import AsyncContainerWriter
from flashflood.workflow.responseworkflow import ResponseWorkflow


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


class GLSNetwork(ResponseWorkflow):
    def __init__(self, contents, params, **kwargs):
        super().__init__(params, **kwargs)
        self.data_type = "edges"
        self.reference["nodes"] = contents["id"]
        self.append(IteratorInput(contents["records"]))
        self.append(MoleculeFromJSON())
        self.append(FunctionNode(functools.partial(gls_array, params)))
        self.append(Combination())
        self.append(CountRows(self.input_size))
        self.append(MPFilter(
            functools.partial(gls_filter, params),
            residue_counter=self.done_count, fields=GRAPH_FIELDS
        ))
        self.append(AsyncCountRows(self.done_count))
        self.append(AsyncContainerWriter(self.results))


class RDKitMorganNetwork(ResponseWorkflow):
    def __init__(self, contents, params, **kwargs):
        super().__init__(params, **kwargs)
        self.data_type = "edges"
        self.reference["nodes"] = contents["id"]
        self.append(IteratorInput(contents["records"]))
        self.append(MoleculeFromJSON())
        self.append(Apply(functools.partial(rdkit_mol, params)))
        self.append(Combination())
        self.append(CountRows(self.input_size))
        self.append(MPFilter(
            functools.partial(morgan_filter, params),
            residue_counter=self.done_count, fields=GRAPH_FIELDS
        ))
        self.append(AsyncCountRows(self.done_count))
        self.append(AsyncContainerWriter(self.results))


class RDKitFMCSNetwork(ResponseWorkflow):
    def __init__(self, contents, params, **kwargs):
        super().__init__(params, **kwargs)
        self.data_type = "edges"
        self.reference["nodes"] = contents["id"]
        self.append(IteratorInput(contents["records"]))
        self.append(MoleculeFromJSON())
        self.append(Apply(functools.partial(rdkit_mol, params)))
        self.append(Combination())
        self.append(CountRows(self.input_size))
        self.append(MPFilter(
            functools.partial(fmcs_filter, params),
            residue_counter=self.done_count, fields=GRAPH_FIELDS
        ))
        self.append(AsyncCountRows(self.done_count))
        self.append(AsyncContainerWriter(self.results))
