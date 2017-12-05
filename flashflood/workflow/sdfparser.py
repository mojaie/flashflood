#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.node.chem.molecule import Molecule
from flashflood.node.function.number import Number
from flashflood.node.reader.sdfile import SDFileLinesInput
from flashflood.node.writer.container import ContainerWriter
from flashflood.workflow.responseworkflow import ResponseWorkflow


class SDFParser(ResponseWorkflow):
    def __init__(self, query, contents, **kwargs):
        super().__init__(query, **kwargs)
        sdf_in = SDFileLinesInput(
            contents, sdf_options=query["params"]["fields"],
            fields=[
                {"key": q, "name": q, "format": "text"}
                for q in query["params"]["fields"]
            ])
        self.append(sdf_in)
        self.append(Molecule())
        self.append(Number())
        self.append(ContainerWriter(self.results))
