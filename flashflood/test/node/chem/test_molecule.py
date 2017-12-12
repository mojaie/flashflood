#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import pickle
import unittest

from chorus.demo import MOL
from chorus import v2000reader as reader
from chorus.model.graphmol import Compound
from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.container import Container
from flashflood.core.task import Task
from flashflood.core.workflow import Workflow
from flashflood.node.chem.molecule import MoleculeToJSON, PickleMolecule
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


class TestMolecule(AsyncTestCase):
    @gen_test
    def test_moltojson(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(
            [{"__molobj": reader.mol_from_text(MOL["demo"])}]))
        wf.append(MoleculeToJSON())
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        rcd = result.records[0]
        out = Compound(json.loads(rcd["__moljson"]))
        self.assertEqual(len(out), 37)

    @gen_test
    def test_picklemol(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(
            [{"__molobj": reader.mol_from_text(MOL["demo"])}]))
        wf.append(PickleMolecule())
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        rcd = result.records[0]
        out = Compound(pickle.loads(rcd["__molpickle"]))
        self.assertEqual(len(out), 37)


if __name__ == '__main__':
    unittest.main()
