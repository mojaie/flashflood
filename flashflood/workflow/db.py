#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.node.chem.molecule import Molecule
from flashflood.node.function.number import Number
from flashflood.node.reader import sqlite
from flashflood.node.writer.container import ContainerWriter
from flashflood.workflow.responseworkflow import ResponseWorkflow


class DBFilter(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        sq_in = sqlite.SQLiteReaderFilter(query)
        number = Number()
        writer = ContainerWriter(self.results)
        self.connect(sq_in, number)
        self.connect(number, writer)


class DBSearch(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        sq_in = sqlite.SQLiteReaderSearch(query)
        number = Number()
        writer = ContainerWriter(self.results)
        self.connect(sq_in, number)
        self.connect(number, writer)


class ChemDBFilter(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        sq_in = sqlite.SQLiteReaderFilter(query)
        molecule = Molecule()
        number = Number()
        writer = ContainerWriter(self.results)
        self.connect(sq_in, molecule)
        self.connect(molecule, number)
        self.connect(number, writer)


class ChemDBSearch(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        sq_in = sqlite.SQLiteReaderSearch(query)
        molecule = Molecule()
        number = Number()
        writer = ContainerWriter(self.results)
        self.connect(sq_in, molecule)
        self.connect(molecule, number)
        self.connect(number, writer)
