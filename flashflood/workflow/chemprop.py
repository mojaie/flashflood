#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#
import functools
import json
import operator
import re

from chorus.model.graphmol import Compound

from flashflood import static
from flashflood.core.workflow import Workflow
from flashflood.node.function.filter import MPFilter
from flashflood.node.chem.molecule import AsyncMolecule
from flashflood.node.function.number import AsyncNumber
from flashflood.node.io.json import AsyncJSONResponse
from flashflood.node.io.sqlite import SQLiteInput


def like_operator(a, b):
    """ regexp implementation of sqlite LIKE operator """
    return re.match(b.replace("%", ".*?").replace("_", "[\w ]"), a) is not None


OPERATORS = {
    "eq": operator.eq, "gt": operator.gt, "lt": operator.lt,
    "ge": operator.ge, "le": operator.le,
    "lk": like_operator, "in": lambda a, b: a in b
}


def prop_filter(func, op, val, row):
    mol = Compound(json.loads(row["_molobj"]))
    try:
        valid = op(func(mol), val)
    except TypeError as e:
        print(e, row["compound_id"], val)
    else:
        if valid:
            return row


class ChemProp(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        sortf = {"numeric": float}
        field = static.CHEM_FIELDS.find("key", query["key"])
        vs = [sortf.get(field["valueType"], str)(v) for v in query["values"]]
        # TODO Is "IN" query necessary?
        v = {True: vs, False: vs[0]}[query["operator"] == "in"]
        func = functools.partial(
            prop_filter,
            static.CHEM_FUNCTIONS[query["key"]],
            OPERATORS[query["operator"]], v
        )
        sq_in = SQLiteInput(query)
        mpf = MPFilter(func)
        molecule = AsyncMolecule()
        number = AsyncNumber()
        response = AsyncJSONResponse(self)
        self.connect(sq_in, mpf)
        self.connect(mpf, molecule)
        self.connect(molecule, number)
        self.connect(number, response)
