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

from flashflood.node.function.filter import MPFilter
from flashflood.node.chem.molecule import AsyncMolecule
from flashflood.node.function.number import AsyncNumber
from flashflood.node.monitor.count import CountRows, AsyncCountRows
from flashflood.node.reader import sqlite
from flashflood.node.writer.container import AsyncContainerWriter
from flashflood.sqlitehelper import SQLITE_HELPER as sq
from flashflood.workflow.responseworkflow import ResponseWorkflow


def exact_filter(qmol, params, row):
    mol = Compound(json.loads(row["__molobj"]))
    if substructure.equal(mol, qmol, ignore_hydrogen=params["ignoreHs"]):
        return row


def substr_filter(qmol, params, row):
    mol = Compound(json.loads(row["__molobj"]))
    if substructure.substructure(
            mol, qmol, ignore_hydrogen=params["ignoreHs"]):
        return row


def supstr_filter(qmol, params, row):
    mol = Compound(json.loads(row["__molobj"]))
    if substructure.substructure(
            qmol, mol, ignore_hydrogen=params["ignoreHs"]):
        return row


class ExactStruct(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        qmol = sq.query_mol(query["queryMol"])
        mw_filter = {
            "targets": query["targets"],
            "key": "_mw_wo_sw",
            "operator": "eq",
            "values": (molutil.mw(qmol),),
        }
        func = functools.partial(exact_filter, qmol, query["params"])
        self.append(sqlite.SQLiteReaderFilter(mw_filter))
        self.append(CountRows(self.input_size))
        self.append(MPFilter(func, residue_counter=self.done_count))
        self.append(AsyncMolecule())
        self.append(AsyncNumber())
        self.append(AsyncCountRows(self.done_count))
        self.append(AsyncContainerWriter(self.results))


class Substruct(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(substr_filter, qmol, query["params"])
        self.append(sqlite.SQLiteReader(query))
        self.append(CountRows(self.input_size))
        self.append(MPFilter(func, residue_counter=self.done_count))
        self.append(AsyncMolecule())
        self.append(AsyncNumber())
        self.append(AsyncCountRows(self.done_count))
        self.append(AsyncContainerWriter(self.results))


class Superstruct(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(supstr_filter, qmol, query["params"])
        self.append(sqlite.SQLiteReader(query))
        self.append(CountRows(self.input_size))
        self.append(MPFilter(func, residue_counter=self.done_count))
        self.append(AsyncMolecule())
        self.append(AsyncNumber())
        self.append(AsyncCountRows(self.done_count))
        self.append(AsyncContainerWriter(self.results))
