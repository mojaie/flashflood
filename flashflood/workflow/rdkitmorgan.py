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

from flashflood import static
from flashflood.node.chem.descriptor import AsyncMolDescriptor
from flashflood.node.chem.molecule import AsyncMoleculeToJSON, UnpickleMolecule
from flashflood.node.function.filter import MPFilter
from flashflood.node.function.number import AsyncNumber
from flashflood.node.monitor.count import CountRows, AsyncCountRows
from flashflood.node.reader.sqlite import SQLiteReader
from flashflood.node.writer.container import AsyncContainerWriter
from flashflood.sqlitehelper import SQLITE_HELPER as sq
from flashflood.workflow.responseworkflow import ResponseWorkflow


def rdmorgan_filter(qmol, params, row):
    mol = Compound(json.loads(row["__molobj"]))
    try:
        score = rdkit.morgan_sim(mol, qmol, radius=2)  # ECFP4 equivalent
    except:
        print(traceback.format_exc())
        return
    thld = float(params["threshold"])
    if score >= thld:
        row["morgan_sim"] = score
        return row


class RDKitMorgan(ResponseWorkflow):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(rdmorgan_filter, qmol, query["params"])
        self.append(SQLiteReader(query))
        self.append(CountRows(self.input_size))
        self.append(UnpickleMolecule())
        self.append(MPFilter(
            func, residue_counter=self.done_count,
            fields=[
                {"key": "morgan_sim", "name": "Fingerprint similarity",
                 "d3_format": ".2f"}
            ]
        ))
        self.append(AsyncMolDescriptor(static.MOL_DESC_KEYS))
        self.append(AsyncMoleculeToJSON())
        self.append(AsyncNumber())
        self.append(AsyncCountRows(self.done_count))
        self.append(AsyncContainerWriter(self.results))
