#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import json

from chorus import substructure
from chorus import molutil
from chorus.model.graphmol import Compound

from flashflood.core.workflow import Workflow
from flashflood.node.function.filter import MPFilter
from flashflood.node.chem.molecule import AsyncMolecule
from flashflood.node.io.json import AsyncJSONResponse
from flashflood.node.function.number import AsyncNumber
from flashflood.node.io import sqlite
from flashflood.sqlitehelper import SQLITE_HELPER as sq


def exact_filter(qmol, params, row):
    mol = Compound(json.loads(row["_molobj"]))
    if substructure.equal(mol, qmol, ignore_hydrogen=params["ignoreHs"]):
        return row


def substr_filter(qmol, params, row):
    mol = Compound(json.loads(row["_molobj"]))
    if substructure.substructure(
            mol, qmol, ignore_hydrogen=params["ignoreHs"]):
        return row


def supstr_filter(qmol, params, row):
    mol = Compound(json.loads(row["_molobj"]))
    if substructure.substructure(
            qmol, mol, ignore_hydrogen=params["ignoreHs"]):
        return row


class ExactStruct(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        qmol = sq.query_mol(query["queryMol"])
        mw_filter = {
            "targets": query["targets"],
            "key": "_mw_wo_sw",
            "operator": "eq",
            "values": (molutil.mw(qmol),),
        }
        func = functools.partial(exact_filter, qmol, query["params"])
        sq_filter = sqlite.SQLiteFilterInput(mw_filter)
        mpf = MPFilter(func)
        molecule = AsyncMolecule()
        number = AsyncNumber()
        response = AsyncJSONResponse(self)
        self.connect(sq_filter, mpf)
        self.connect(mpf, molecule)
        self.connect(molecule, number)
        self.connect(number, response)


class Substruct(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(substr_filter, qmol, query["params"])
        sq_in = sqlite.SQLiteInput(query)
        mpf = MPFilter(func)
        molecule = AsyncMolecule()
        number = AsyncNumber()
        response = AsyncJSONResponse(self)
        self.connect(sq_in, mpf)
        self.connect(mpf, molecule)
        self.connect(molecule, number)
        self.connect(number, response)


class Superstruct(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(supstr_filter, qmol, query["params"])
        sq_in = sqlite.SQLiteInput(query)
        mpf = MPFilter(func)
        molecule = AsyncMolecule()
        number = AsyncNumber()
        response = AsyncJSONResponse(self)
        self.connect(sq_in, mpf)
        self.connect(mpf, molecule)
        self.connect(molecule, number)
        self.connect(number, response)
