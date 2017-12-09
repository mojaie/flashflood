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
from flashflood.node.chem.molecule import MoleculeToJSON, PickleMolecule
from flashflood.node.reader.iterator import IteratorInput
from flashflood.node.writer.container import ContainerWriter


class TestMolecule(AsyncTestCase):
    @gen_test
    def test_moltojson(self):
        result = Container()
        iter_in = IteratorInput(
            [{"__molobj": reader.mol_from_text(MOL["demo"])}])
        mol = MoleculeToJSON()
        self.assertIsInstance(pickle.dumps(mol.func), bytes)
        out = ContainerWriter(result)
        mol.add_in_edge(iter_in.out_edge(0), 0)
        out.add_in_edge(mol.out_edge(0), 0)
        iter_in.on_submitted()
        mol.on_submitted()
        out.on_submitted()
        iter_in.run()
        mol.run()
        yield out.run()
        rcd = result.records[0]
        out = Compound(json.loads(rcd["__moljson"]))
        self.assertEqual(len(out), 37)
        self.assertEqual(mol.status, "done")

    @gen_test
    def test_picklemol(self):
        result = Container()
        iter_in = IteratorInput(
            [{"__molobj": reader.mol_from_text(MOL["demo"])}])
        mol = PickleMolecule()
        self.assertIsInstance(pickle.dumps(mol.func), bytes)
        out = ContainerWriter(result)
        mol.add_in_edge(iter_in.out_edge(0), 0)
        out.add_in_edge(mol.out_edge(0), 0)
        iter_in.on_submitted()
        mol.on_submitted()
        out.on_submitted()
        iter_in.run()
        mol.run()
        yield out.run()
        rcd = result.records[0]
        out = Compound(pickle.loads(rcd["__molpickle"]))
        self.assertEqual(len(out), 37)
        self.assertEqual(mol.status, "done")


if __name__ == '__main__':
    unittest.main()
