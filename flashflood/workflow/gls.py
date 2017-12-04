#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import json

from chorus import mcsdr
from chorus.model.graphmol import Compound

from flashflood.node.function.filter import MPFilter
from flashflood.node.chem.molecule import AsyncMolecule
from flashflood.node.function.number import AsyncNumber
from flashflood.node.io.sqlite import SQLiteInput
from flashflood.node.writer.container import AsyncContainerWriter
from flashflood.sqlitehelper import SQLITE_HELPER as sq
from flashflood.workflow.responseworkflow import ResponseWorkflow


def mcsdr_filter(qmolarr, params, row):
    mol = Compound(json.loads(row["_molobj"]))
    type_ = {"sim": "local_sim", "edge": "mcsdr_edges"}
    if len(mol) > params["molSizeCutoff"]:  # mol size filter
        return
    try:
        arr = mcsdr.comparison_array(
            mol, params["diameter"], params["maxTreeSize"])
    except ValueError:
        return
    sm, bg = sorted([arr[1], qmolarr[1]])
    thld = float(params["threshold"])
    if params["measure"] == "sim" and sm < bg * thld:
        return  # threshold filter
    if params["measure"] == "edge" and sm < thld:
        return  # fragment size filter
    res = mcsdr.local_sim(arr, qmolarr)
    if res[type_[params["measure"]]] >= thld:
        row["_local_sim"] = res["local_sim"]
        row["_mcsdr"] = res["mcsdr_edges"]
        return row


class GLS(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        self.fields.extend([
            {"key": "_mcsdr", "name": "MCS-DR size", "d3_format": "d"},
            {"key": "_local_sim", "name": "GLS", "d3_format": ".2f"}
        ])
        qmol = sq.query_mol(query["queryMol"])
        qmolarr = mcsdr.comparison_array(
            qmol, query["params"]["diameter"], query["params"]["maxTreeSize"])
        func = functools.partial(mcsdr_filter, qmolarr, query["params"])
        sq_in = SQLiteInput(query)
        mpf = MPFilter(func)
        molecule = AsyncMolecule()
        number = AsyncNumber()
        writer = AsyncContainerWriter(self.results)
        self.connect(sq_in, mpf)
        self.connect(mpf, molecule)
        self.connect(molecule, number)
        self.connect(number, writer)
