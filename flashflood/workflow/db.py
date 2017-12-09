#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood import static
from flashflood.node.chem.descriptor import MolDescriptor
from flashflood.node.chem.molecule import MoleculeToJSON, UnpickleMolecule
from flashflood.node.function.number import Number
from flashflood.node.reader import sqlite
from flashflood.node.writer.container import ContainerWriter
from flashflood.workflow.responseworkflow import ResponseWorkflow


class DBFilter(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        self.append(sqlite.SQLiteReaderFilter(query))
        self.append(Number())
        self.append(ContainerWriter(self.results))


class DBSearch(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        self.append(sqlite.SQLiteReaderSearch(query))
        self.append(Number())
        self.append(ContainerWriter(self.results))


class ChemDBFilter(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        self.append(sqlite.SQLiteReaderFilter(query))
        self.append(UnpickleMolecule())
        self.append(MolDescriptor(static.MOL_DESC_KEYS))
        self.append(MoleculeToJSON())
        self.append(Number())
        self.append(ContainerWriter(self.results))


class ChemDBSearch(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        self.append(sqlite.SQLiteReaderSearch(query))
        self.append(UnpickleMolecule())
        self.append(MolDescriptor(static.MOL_DESC_KEYS))
        self.append(MoleculeToJSON())
        self.append(Number())
        self.append(ContainerWriter(self.results))
