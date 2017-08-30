#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import json
from itertools import repeat
import pickle
import unittest

from kiwiii import tablefilter as tf

from chorus.model.graphmol import Compound
from chorus.demo import MOL
from chorus import v2000reader

RESOURCE_DIR = os.path.join(
    os.path.dirname(__file__),
    "../../resources"
)


class TestTableFilter(unittest.TestCase):
    def test_first_match_filter(self):
        query = {
            "method": "sql",
            "targets": ["text_demo:TEST1_LIB1"],
            "key": "ID",
            "values": ["DB00186", "DB00193", "DB00000", "DB01143"],
            "operator": "fm"
        }
        flt = tf.FirstMatchFilter(query)
        args = flt.args_generator()
        arg1 = next(args)
        self.assertEqual(arg1["IC50"], 2.00E-5)
        arg2 = next(args)
        self.assertEqual(arg2["FP"], -11.1)
        arg3 = next(args)
        self.assertEqual(arg3, {"ID": "DB00000"})

    def test_chem_first_match_filter(self):
        query = {
            "method": "sql",
            "targets": ["sdf_demo:DRUGBANKFDA"],
            "key": "ID",
            "values": ["DB00928", "DB00930", "DB00000"],
            "operator": "fm"
        }
        flt = tf.ChemFirstMatchFilter(query)
        args = flt.args_generator()
        arg1 = next(args)
        self.assertEqual(arg1["NAME"], "Azacitidine")
        arg2 = next(args)  # No structure
        self.assertEqual(arg2["NAME"], "Colesevelam")
        self.assertEqual(arg2["_mw_wo_sw"], 0)
        mol2 = Compound(pickle.loads(arg2["_mol"]))
        self.assertEqual(len(mol2), 0)
        arg3 = next(args)  # Null record (not found)
        self.assertEqual(arg3["ID"], "DB00000")
        self.assertEqual(arg3.get("NAME"), None)
        self.assertEqual(arg3.get("_mw_wo_sw"), None)
        mol3 = Compound(pickle.loads(arg3["_mol"]))
        self.assertEqual(len(mol3), 0)

    def test_chem_db_query_filter_eq(self):
        query = {
            "method": "chemsql",
            "targets": ["sdf_demo:DRUGBANKFDA"],
            "key": "ID",
            "values": ["DB00928"],
            "operator": "eq"
        }
        flt = tf.ChemFindAllFilter(query)
        # write
        data = {"columns": [], "records": []}
        flt.initialize(data)
        # args_generator
        arg = next(flt.args_generator())
        self.assertEqual(arg["NAME"], "Azacitidine")
        self.assertEqual(arg["_mw_wo_sw"], 244.21)  # pre-calculated
        mol1 = Compound(pickle.loads(arg["_mol"]))  # pickled molecule
        self.assertEqual(len(mol1), 17)
        # row_processor
        func = flt.row_processor
        row = {}
        done = func(row, arg)
        self.assertTrue(done)
        self.assertEqual(row["NAME"], "Azacitidine")
        self.assertEqual(row["_mw_wo_sw"], 244.21)
        mol2 = Compound(json.loads(row["_mol"]))  # JSON serialized molecule
        self.assertEqual(len(mol2), 17)

    def test_chem_prop_filter_num(self):
        query = {
            "method": "chemsql",
            "targets": ["sdf_demo:DRUGBANKFDA"],
            "key": "_mw",
            "values": [1200],
            "operator": "gt"
        }
        flt = tf.ChemPropFilter(query)
        # write
        data = {"columns": [], "records": []}
        flt.initialize(data)
        self.assertEqual(data["searchCount"], 1543)
        # row_processor
        func = flt.row_processor
        args = flt.args_generator()
        arg = next(args)
        row = {}
        self.assertTrue(func(row, arg))
        self.assertEqual(row["NAME"], "Goserelin")
        self.assertEqual(row["_mw"], 1269.43)
        mol = Compound(json.loads(row["_mol"]))  # JSON serialized molecule
        self.assertEqual(len(mol), 91)
        arg2 = next(args)
        row2 = {}
        self.assertFalse(func(row2, arg2))  # Desmopressin mwwosw:1069.23
        # res_count = sum(map(func, repeat({}), flt.args_generator()))
        # self.assertEqual(res_count, 29)

    def test_substruct_filter_smiles(self):
        query = {
            "method": "sub",
            "targets": ["sdf_demo:DRUGBANKFDA"],
            "format": "smiles",
            "value": "C1=CC=CC=C1CC(C(=O)OC)NC(=O)C(N)CC(=O)O",
            "ignoreHs": True
        }
        flt = tf.SubStructFilter(query)
        # write
        data = {"columns": [], "records": []}
        flt.initialize(data)
        self.assertEqual(data["searchCount"], 1543)
        # row_processor
        func = flt.row_processor
        row = {}
        task = map(func, repeat(row), flt.args_generator())
        next(filter(lambda x: x, task))
        self.assertEqual(row["NAME"], "L-Phenylalanine")

    def test_exact_filter_dbid(self):
        query = {
            "method": "exact",
            "targets": ["sdf_demo:DRUGBANKALL"],
            "format": "dbid",
            "querySource": "sdf_demo:DRUGBANKFDA",
            "value": "DB00928",
            "ignoreHs": True
        }
        flt = tf.ExactStructFilter(query)
        # row_processor
        func = flt.row_processor
        row = {}
        task = map(func, repeat(row), flt.args_generator())
        next(filter(lambda x: x, task))
        self.assertEqual(row["NAME"], "Azacitidine")

    def test_gls_filter_molfile(self):
        query = {
            "method": "gls",
            "targets": ["sdf_demo:DRUGBANKALL"],
            "format": "molfile",
            "value": MOL["Phe"],
            "threshold": 0.7,
            "ignoreHs": True,
            "diameter": 8,
            "maxTreeSize": 30,
            "molSizeCutoff": 100,
        }
        flt = tf.GlsFilter(query)
        # row_processor
        func = flt.row_processor
        row = {}
        task = map(func, repeat(row), flt.args_generator())
        next(filter(lambda x: x, task))
        self.assertEqual(row["NAME"], "L-Phenylalanine")

    @unittest.skip('This takes 1 sec.')
    def test_rdfmcs_filter_molfile(self):
        query = {
            "method": "rdfmcs",
            "targets": ["sdf_demo:DRUGBANKFDA"],
            "format": "molfile",
            "value": MOL["Phe"],
            "thresholdType": "edge",
            "threshold": 9,
            "ignoreHs": True,
            "timeout": 1,
        }
        flt = tf.RdFmcsFilter(query)
        # row_processor
        func = flt.row_processor
        row = {}
        task = map(func, repeat(row), flt.args_generator())
        next(filter(lambda x: x, task))
        self.assertEqual(row["NAME"], "Goserelin")

    def test_sdf_filter(self):
        sdf = []
        path = os.path.join(
            RESOURCE_DIR,
            "sdf_demo/DrugBank_FDA_Approved_1-20.sdf"
        )
        with open(path, 'r') as f:
            [sdf.append(line) for line in f]
        query = {
            "columns": ["DRUGBANK_ID", "GENERIC_NAME", "BRANDS"],
            "implh": False,
            "recalc": False
        }
        flt = tf.SdfFilter("".join(sdf), query)
        # row_processor
        func = flt.row_processor
        args = flt.args_generator()
        arg = next(args)
        row = {}
        self.assertTrue(func(row, arg))
        self.assertEqual(row["DRUGBANK_ID"], "DB00014")
        self.assertEqual(row["GENERIC_NAME"], "Goserelin")
        mol = Compound(json.loads(row["_mol"]))
        self.assertEqual(len(mol), 91)

    def test_gls_dist_matrix(self):
        sup = map(
            lambda x: json.dumps(x.jsonized()),
            v2000reader.mols_from_file(
                os.path.join(
                    RESOURCE_DIR,
                    "sdf_demo/DrugBank_FDA_Approved_1-20.sdf"
                )
            )
        )
        query = {
            "nodeTableId": "VALID_UUID",
            "indices": range(20),
            "molecules": sup,
            "measure": "gls",
            "threshold": 0.9,
            "ignoreHs": True,
            "diameter": 8,
            "maxTreeSize": 40,
            "molSizeCutoff": 100,
        }
        flt = tf.GlsMatrixFilter(query)
        data = {"columns": [], "records": []}
        flt.initialize(data)
        # nodes * (nodes - 1) / 2 - 19(number of invalid nodes)
        self.assertEqual(data["searchCount"], 171)
        # row_processor
        func = flt.row_processor
        row = {}
        res = filter(None, map(func, repeat(row), flt.args_generator()))
        next(res)
        self.assertEqual(row, {"source": 0, "target": 5, "weight": 1.00})
        next(res)
        self.assertEqual(row, {"source": 1, "target": 4, "weight": 1.00})


if __name__ == '__main__':
    unittest.main()
