#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.workflow import Workflow
from flashflood.node.chem.molecule import Molecule
from flashflood.node.function.number import Number
from flashflood.node.io import sqlite
from flashflood.node.io.json import JSONResponse


class DBFilter(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        sq_in = sqlite.SQLiteFilterInput(query)
        number = Number()
        response = JSONResponse(self)
        self.connect(sq_in, number)
        self.connect(number, response)


class DBSearch(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        sq_in = sqlite.SQLiteSearchInput(query)
        number = Number()
        response = JSONResponse(self)
        self.connect(sq_in, number)
        self.connect(number, response)


class ChemDBFilter(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        sq_in = sqlite.SQLiteFilterInput(query)
        molecule = Molecule()
        number = Number()
        response = JSONResponse(self)
        self.connect(sq_in, molecule)
        self.connect(molecule, number)
        self.connect(number, response)


class ChemDBSearch(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        sq_in = sqlite.SQLiteSearchInput(query)
        molecule = Molecule()
        number = Number()
        response = JSONResponse(self)
        self.connect(sq_in, molecule)
        self.connect(molecule, number)
        self.connect(number, response)
