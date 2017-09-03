#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.workflow import Workflow
from flashflood.node.chem.molecule import Molecule
from flashflood.node.function.number import Number
from flashflood.node.io.json import JSONResponse
from flashflood.node.io.sdfile import SDFileLinesInput


class SDFParser(Workflow):
    def __init__(self, contents, query):
        super().__init__()
        self.query = query
        sdf_in = SDFileLinesInput(
            contents, sdf_options=query["params"]["fields"],
            fields=[
                {"key": q, "name": q, "valueType": "text"}
                for q in query["params"]["fields"]
            ])
        molecule = Molecule()
        number = Number()
        response = JSONResponse(self)
        self.connect(sdf_in, molecule)
        self.connect(molecule, number)
        self.connect(number, response)
