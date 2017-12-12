#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood import static
from flashflood.core.container import Container
from flashflood.core.workflow import Workflow
from flashflood.node.chem.descriptor import MolDescriptor
from flashflood.node.chem.molecule import MoleculeToJSON, UnpickleMolecule
from flashflood.node.field.number import Number
from flashflood.node.reader import sqlite
from flashflood.node.writer.container import ContainerWriter


class DBFilter(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.data_type = "nodes"
        self.append(sqlite.SQLiteReaderFilter(query))
        self.append(Number("index", fields=[static.INDEX_FIELD]))
        self.append(ContainerWriter(self.results))


class DBSearch(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.data_type = "nodes"
        self.append(sqlite.SQLiteReaderSearch(query))
        self.append(Number("index", fields=[static.INDEX_FIELD]))
        self.append(ContainerWriter(self.results))


class ChemDBFilter(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.data_type = "nodes"
        self.append(sqlite.SQLiteReaderFilter(query))
        self.append(UnpickleMolecule())
        self.append(MolDescriptor(static.MOL_DESC_KEYS))
        self.append(MoleculeToJSON())
        self.append(Number("index", fields=[static.INDEX_FIELD]))
        self.append(ContainerWriter(self.results))


class ChemDBSearch(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.data_type = "nodes"
        self.append(sqlite.SQLiteReaderSearch(query))
        self.append(UnpickleMolecule())
        self.append(MolDescriptor(static.MOL_DESC_KEYS))
        self.append(MoleculeToJSON())
        self.append(Number("index", fields=[static.INDEX_FIELD]))
        self.append(ContainerWriter(self.results))
