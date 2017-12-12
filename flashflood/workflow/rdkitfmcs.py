#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import traceback

from chorus import rdkit

from flashflood import static
from flashflood.core.concurrent import ConcurrentFilter
from flashflood.core.container import Container, Counter
from flashflood.core.workflow import Workflow
from flashflood.node.chem.descriptor import AsyncMolDescriptor
from flashflood.node.chem.molecule import AsyncMoleculeToJSON, UnpickleMolecule
from flashflood.node.field.number import AsyncNumber
from flashflood.node.monitor.count import AsyncCountRows
from flashflood.node.reader.sqlite import SQLiteReader
from flashflood.node.writer.container import ContainerWriter
from flashflood.sqlitehelper import SQLITE_HELPER as sq


def rdfmcs_filter(qmol, params, row):
    type_ = {"sim": "similarity", "edge": "mcs_edges"}
    try:
        res = rdkit.fmcs(row["__molobj"], qmol, timeout=params["timeout"])
    except:
        print(traceback.format_exc())
        return
    thld = float(params["threshold"])
    if res[type_[params["measure"]]] >= thld:
        row["fmcs_sim"] = res["similarity"]
        row["fmcs_edges"] = res["mcs_edges"]
        return row


class RDKitFMCS(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "nodes"
        self.fields.extend([
            {"key": "fmcs_sim", "name": "MCS similarity", "d3_format": ".2f"},
            {"key": "fmcs_edges", "name": "MCS size", "d3_format": "d"}
        ])
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(rdfmcs_filter, qmol, query["params"])
        self.append(SQLiteReader(query, counter=self.input_size))
        self.append(UnpickleMolecule())
        self.append(ConcurrentFilter(
            func=func, residue_counter=self.done_count))
        self.append(AsyncMolDescriptor(static.MOL_DESC_KEYS))
        self.append(AsyncMoleculeToJSON())
        self.append(AsyncNumber("index", fields=[static.INDEX_FIELD]))
        self.append(AsyncCountRows(self.done_count))
        self.append(ContainerWriter(self.results))
