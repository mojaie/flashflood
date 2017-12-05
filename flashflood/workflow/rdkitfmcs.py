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

from flashflood.node.function.filter import MPFilter
from flashflood.node.chem.molecule import AsyncMolecule
from flashflood.node.function.number import AsyncNumber
from flashflood.node.monitor.count import CountRows, AsyncCountRows
from flashflood.node.reader.sqlite import SQLiteReader
from flashflood.node.writer.container import AsyncContainerWriter
from flashflood.sqlitehelper import SQLITE_HELPER as sq
from flashflood.workflow.responseworkflow import ResponseWorkflow


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


class RDKitFMCS(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        self.fields.extend([
            {"key": "_fmcs_sim", "name": "MCS similarity", "d3_format": ".2f"},
            {"key": "_fmcs_edges", "name": "MCS size", "d3_format": "d"}
        ])
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(rdfmcs_filter, qmol, query["params"])
        sq_in = SQLiteReader(query)
        count_in = CountRows(self.input_size)
        mpf = MPFilter(func, residue_counter=self.done_count)
        molecule = AsyncMolecule()
        number = AsyncNumber()
        count_out = AsyncCountRows(self.done_count)
        writer = AsyncContainerWriter(self.results)
        self.connect(sq_in, count_in)
        self.connect(count_in, mpf)
        self.connect(mpf, molecule)
        self.connect(molecule, number)
        self.connect(number, count_out)
        self.connect(count_out, writer)
