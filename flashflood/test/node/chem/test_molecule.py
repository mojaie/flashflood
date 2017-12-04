#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import unittest

from chorus.demo import MOL
from chorus import v2000reader as reader
from chorus.model.graphmol import Compound

from flashflood.node.chem.molecule import Molecule
from flashflood.node.reader.iterator import IteratorInput


class TestMolecule(unittest.TestCase):
    def test_molecule(self):
        iter_in = IteratorInput([reader.mol_from_text(MOL["demo"])])
        mol = Molecule()
        mol.add_in_edge(iter_in.out_edge(), 0)
        iter_in.on_submitted()
        mol.on_submitted()
        iter_in.run()
        yield mol.run()
        rcd = mol.out_edge(0).records[0]
        out = Compound(json.loads(rcd["_molobj"]))
        self.assertEqual(len(out), 20)
        self.assertAlmostEqual(rcd["_mw"], 754.7)
        self.assertEqual(mol.status, "done")


if __name__ == '__main__':
    unittest.main()
