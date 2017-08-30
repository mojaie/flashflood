#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import unittest

from kiwiii import tablecolumn as tc

from chorus.demo import MOL
from chorus import v2000reader


class TestTableColumn(unittest.TestCase):
    def test_index_column(self):
        flt = tc.IndexColumn()
        args = flt.args_generator()
        arg = next(args)
        row = {}
        self.assertTrue(flt.row_processor(row, arg))
        self.assertTrue(row["_index"], 1)

    def test_struct_column(self):
        flt = tc.StructureColumn()
        mol = v2000reader.mol_from_text(MOL["Ceftazidime"])
        row = {"_mol": json.dumps(mol.jsonized())}
        self.assertTrue(flt.row_processor(row, None))

    def test_calc_column_group(self):
        flt = tc.CalcColumnGroup()
        mol = v2000reader.mol_from_text(MOL["Ceftazidime"])
        row = {"_mol": json.dumps(mol.jsonized())}
        self.assertTrue(flt.row_processor(row, None))
        self.assertTrue(row["_mw"], 546.58)
        self.assertTrue(row["_formula"], "C22H21N6O7S2")
        self.assertTrue(row["_logp"], -1.3)


if __name__ == '__main__':
    unittest.main()
