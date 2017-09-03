#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import json
import traceback

from chorus import rdkit
from chorus.model.graphmol import Compound

from flashflood.core.workflow import Workflow
from flashflood.node.function.filter import MPFilter
from flashflood.node.chem.molecule import AsyncMolecule
from flashflood.node.function.number import AsyncNumber
from flashflood.node.io.json import AsyncJSONResponse
from flashflood.node.io.sqlite import SQLiteInput
from flashflood.sqlitehelper import SQLITE_HELPER as sq


def rdfmcs_filter(qmol, params, row):
    mol = Compound(json.loads(row["_molobj"]))
    type_ = {"sim": "similarity", "edge": "mcs_edges"}
    try:
        res = rdkit.fmcs(mol, qmol, timeout=params["timeout"])
    except:
        print(traceback.format_exc())
        return
    thld = float(params["threshold"])
    if res[type_[params["measure"]]] >= thld:
        row["_fmcs_sim"] = res["similarity"]
        row["_fmcs_edges"] = res["mcs_edges"]
        return row


class RDKitFMCS(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields.extend([
            {"key": "_fmcs_sim", "name": "MCS size", "sortType": "numeric"},
            {"key": "_fmcs_edges", "name": "MCS similarity",
             "sortType": "numeric"}
        ])
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(rdfmcs_filter, qmol, query["params"])
        sq_in = SQLiteInput(query)
        mpf = MPFilter(func)
        molecule = AsyncMolecule()
        number = AsyncNumber()
        response = AsyncJSONResponse(self)
        self.connect(sq_in, mpf)
        self.connect(mpf, molecule)
        self.connect(molecule, number)
        self.connect(number, response)
