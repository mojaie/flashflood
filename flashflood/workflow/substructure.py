#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from chorus import substructure
from chorus import molutil

from flashflood import static
from flashflood.core.concurrent import ConcurrentFilter
from flashflood.core.container import Container, Counter
from flashflood.core.workflow import Workflow
from flashflood.node.chem.descriptor import MolDescriptor, AsyncMolDescriptor
from flashflood.node.chem.molecule import (
    MoleculeToJSON, AsyncMoleculeToJSON, UnpickleMolecule)
from flashflood.node.control.filter import Filter
from flashflood.node.field.number import Number, AsyncNumber
from flashflood.node.monitor.count import CountRows, AsyncCountRows
from flashflood.node.reader import sqlite
from flashflood.node.writer.container import ContainerWriter
from flashflood.sqlitehelper import SQLITE_HELPER as sq


def exact_filter(qmol, params, row):
    if substructure.equal(
            row["__molobj"], qmol, ignore_hydrogen=params["ignoreHs"]):
        return row


def substr_filter(qmol, params, row):
    if substructure.substructure(
            row["__molobj"], qmol, ignore_hydrogen=params["ignoreHs"]):
        return row


def supstr_filter(qmol, params, row):
    if substructure.substructure(
            qmol, row["__molobj"], ignore_hydrogen=params["ignoreHs"]):
        return row


class ExactStruct(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "nodes"
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
        self.append(UnpickleMolecule())
        self.append(Filter(func=func, residue_counter=self.done_count))
        self.append(MolDescriptor(static.MOL_DESC_KEYS))
        self.append(MoleculeToJSON())
        self.append(Number("index", fields=[static.INDEX_FIELD]))
        self.append(CountRows(self.done_count))
        self.append(ContainerWriter(self.results))


class Substruct(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "nodes"
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(substr_filter, qmol, query["params"])
        self.append(sqlite.SQLiteReader(query, counter=self.input_size))
        self.append(UnpickleMolecule())
        self.append(ConcurrentFilter(
            func=func, residue_counter=self.done_count))
        self.append(AsyncMolDescriptor(static.MOL_DESC_KEYS))
        self.append(AsyncMoleculeToJSON())
        self.append(AsyncNumber("index", fields=[static.INDEX_FIELD]))
        self.append(AsyncCountRows(self.done_count))
        self.append(ContainerWriter(self.results))


class Superstruct(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "nodes"
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(supstr_filter, qmol, query["params"])
        self.append(sqlite.SQLiteReader(query, counter=self.input_size))
        self.append(UnpickleMolecule())
        self.append(ConcurrentFilter(
            func=func, residue_counter=self.done_count))
        self.append(AsyncMolDescriptor(static.MOL_DESC_KEYS))
        self.append(AsyncMoleculeToJSON())
        self.append(AsyncNumber("index", fields=[static.INDEX_FIELD]))
        self.append(AsyncCountRows(self.done_count))
        self.append(ContainerWriter(self.results))
