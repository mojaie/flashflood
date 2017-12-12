#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood import static
from flashflood.core.container import Container
from flashflood.core.workflow import Workflow
from flashflood.node.chem.descriptor import MolDescriptor
from flashflood.node.chem.molecule import MoleculeToJSON
from flashflood.node.field.number import Number
from flashflood.node.reader.sdfile import SDFileLinesInput
from flashflood.node.writer.container import ContainerWriter


class SDFParser(Workflow):
    def __init__(self, contents, query, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.results = Container()
        self.data_type = "nodes"
        self.append(SDFileLinesInput(
            contents, sdf_options=query["params"]["fields"],
            fields=[
                {"key": q, "name": q, "format": "text"}
                for q in query["params"]["fields"]
            ]
        ))
        self.append(MolDescriptor(static.MOL_DESC_KEYS))
        self.append(MoleculeToJSON())
        self.append(Number("index", fields=[static.INDEX_FIELD]))
        self.append(ContainerWriter(self.results))
