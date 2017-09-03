#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import json

from chorus import mcsdr
from chorus import molutil
from chorus import rdkit
from chorus.model.graphmol import Compound

from flashflood.core.workflow import Workflow
from flashflood.node.function.filter import MPFilter
from flashflood.node.io.json import AsyncJSONResponse
from flashflood.node.io.iterator import IteratorInput
from flashflood.node.transform.combination import Combination


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
    mol = Compound(json.loads(rcd["_molobj"]))
    if params["ignoreHs"]:
        mol = molutil.make_Hs_implicit(mol)
    diam = int(params["diameter"])
    tree = int(params["maxTreeSize"])
    arr = mcsdr.comparison_array(mol, diam, tree)
    return {"index": rcd["_index"], "array": arr}


def rdkit_mol(params, rcd):
    mol = Compound(json.loads(rcd["_molobj"]))
    if params["ignoreHs"]:
        mol = molutil.make_Hs_implicit(mol)
    return {"index": rcd["_index"], "mol": mol}


class GLSNetwork(Workflow):
    def __init__(self, contents, params):
        super().__init__()
        self.datatype = "edges"
        self.nodesid = contents["id"]
        self.query = params
        self.fields.merge([
            {"key": "source", "name": "source", "valueType": "numeric"},
            {"key": "target", "name": "target", "valueType": "numeric"},
            {"key": "weight", "name": "weight", "valueType": "numeric"}
        ])
        arrgen = functools.partial(gls_array, params)
        arrs = map(arrgen, contents["records"])
        filter_ = functools.partial(gls_filter, params)
        iter_in = IteratorInput(arrs)
        comb = Combination()
        mpf = MPFilter(filter_)
        response = AsyncJSONResponse(self)
        self.connect(iter_in, comb)
        self.connect(comb, mpf)
        self.connect(mpf, response)


class RDKitMorganNetwork(Workflow):
    def __init__(self, contents, params):
        super().__init__()
        self.datatype = "edges"
        self.nodesid = contents["id"]
        self.query = params
        self.fields.merge([
            {"key": "source"},
            {"key": "target"},
            {"key": "weight"}
        ])
        mols = map(functools.partial(rdkit_mol, params), contents["records"])
        filter_ = functools.partial(morgan_filter, params)
        iter_in = IteratorInput(mols)
        comb = Combination()
        mpf = MPFilter(filter_)
        response = AsyncJSONResponse(self)
        self.connect(iter_in, comb)
        self.connect(comb, mpf)
        self.connect(mpf, response)


class RDKitFMCSNetwork(Workflow):
    def __init__(self, contents, params):
        super().__init__()
        self.datatype = "edges"
        self.nodesid = contents["id"]
        self.query = params
        self.fields.merge([
            {"key": "source"},
            {"key": "target"},
            {"key": "weight"}
        ])
        mols = map(functools.partial(rdkit_mol, params), contents["records"])
        filter_ = functools.partial(fmcs_filter, params)
        iter_in = IteratorInput(mols)
        comb = Combination()
        mpf = MPFilter(filter_)
        response = AsyncJSONResponse(self)
        self.connect(iter_in, comb)
        self.connect(comb, mpf)
        self.connect(mpf, response)
