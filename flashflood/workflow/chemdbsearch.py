#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood import static
from flashflood.core.container import Container
from flashflood.core.workflow import Workflow
from flashflood.interface import sqlite
from flashflood.node.chem.descriptor import MolDescriptor
from flashflood.node.chem.molecule import MoleculeToJSON, UnpickleMolecule
from flashflood.node.field.number import Number
from flashflood.node.reader.sqlite import SQLiteReaderSearch
from flashflood.node.writer.container import ContainerWriter


class ChemDBSearch(Workflow):
    def __init__(self, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.data_type = "nodes"
        self.append(SQLiteReaderSearch(
            [sqlite.find_resource(t) for t in query["targets"]],
            query["key"], query["values"],
            fields=sqlite.merged_fields(query["targets"])
            ))
        self.append(UnpickleMolecule())
        self.append(MolDescriptor(static.MOL_DESC_KEYS))
        self.append(MoleculeToJSON())
        self.append(Number("index", fields=[static.INDEX_FIELD]))
        self.append(ContainerWriter(self.results))
