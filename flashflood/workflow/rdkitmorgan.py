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
from flashflood.interface import sqlite
from flashflood.node.chem.descriptor import AsyncMolDescriptor
from flashflood.node.chem.molecule import AsyncMoleculeToJSON, UnpickleMolecule
from flashflood.node.field.number import AsyncNumber
from flashflood.node.monitor.count import AsyncCountRows
from flashflood.node.reader.sqlite import SQLiteReader
from flashflood.node.writer.container import ContainerWriter


def rdmorgan_calc(qmol, radius, row):
    try:
        row["morgan_sim"] = rdkit.morgan_sim(row["__molobj"], qmol, radius)
    except:
        print(traceback.format_exc())
        return
    return row


def thld_filter(thld, row):
    return row["morgan_sim"] >= thld


class RDKitMorgan(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.done_count = Counter()
        self.input_size = Counter()
        self.data_type = "nodes"
        thld = float(query["params"]["threshold"])
        self.append(SQLiteReader(
            [sqlite.find_resource(t) for t in query["targets"]],
            fields=sqlite.merged_fields(query["targets"]),
            counter=self.input_size
        ))
        self.append(UnpickleMolecule())
        # radius=2 is ECFP4 equivalent
        qmol = sqlite.query_mol(query["queryMol"])
        self.append(ConcurrentFilter(
            functools.partial(thld_filter, thld),
            func=functools.partial(rdmorgan_calc, qmol, 2),
            residue_counter=self.done_count,
            fields=[
                {"key": "morgan_sim", "name": "Fingerprint similarity",
                 "d3_format": ".2f"}
            ]
        ))
        self.append(AsyncMolDescriptor(static.MOL_DESC_KEYS))
        self.append(AsyncMoleculeToJSON())
        self.append(AsyncNumber("index", fields=[static.INDEX_FIELD]))
        self.append(AsyncCountRows(self.done_count))
        self.append(ContainerWriter(self.results))
