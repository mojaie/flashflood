#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import pickle
import unittest
import json

from kiwiii import tablebuilder as tb
from kiwiii import tablefilter as tf
from kiwiii import tablecolumn as tc
from kiwiii.util import debug
from tornado import testing as tt
from chorus import v2000reader


class TestTableBuilder(unittest.TestCase):
    def test_builder(self):
        builder = tb.TableBuilder()
        query = {
            "method": "sql",
            "targets": ["sdf_demo:DRUGBANKFDA"],
            "key": "_mw",
            "values": [1200],
            "operator": "gt"
        }
        flt = tf.ChemPropFilter(query)
        # builder and columns should be picklable for parallel computation
        builder.add_filter(flt)
        builder.add_filter(tc.IndexColumn())
        builder.add_filter(tc.StructureColumn())
        builder.add_filter(tc.CalcColumnGroup())
        pickle.dumps(tb.filter_cascade)

    def test_superstruct_run(self):
        builder = tb.TableBuilder()
        query = {
            "method": "sql",
            "targets": ["sdf_demo:DRUGBANKFDA"],
            "format": "smiles",
            "value": "CCCCCCCCCCCCCCC",
            "ignoreHs": True
        }
        flt = tf.SuperStructFilter(query)
        builder.add_filter(flt)
        args = zip(*builder.args_generators)
        while True:
            arg = next(args)
            res = tb.filter_cascade(builder.row_processors, arg)
            if res is not None:
                self.assertEqual(res["NAME"], "Vitamin E")
                break


class TestPerformance(tt.AsyncTestCase):
    @unittest.skip("")
    @debug.profile
    @tt.gen_test(timeout=30)
    def test_performance(self):
        builder = tb.TableBuilder()
        flt = tf.SuperStructFilter(
            "sdf_demo.sqlite3",
            ("DrugBank_All",),
            "CCCCCCCCCCCCCCC",
            "smiles", None
        )
        builder.add_filter(flt)
        worker = tb.RowWiseWorker(
            zip(*builder.args_generators), builder.filter_cascade,
            builder.data
        )
        yield worker.run()
        self.assertEqual(len(builder.data["records"]), 19)

    @unittest.skip("")
    @tt.gen_test(timeout=120)
    def test_graph_performance(self):
        sup = map(
            lambda x: json.dumps(x.jsonized()),
            v2000reader.mols_from_file(
                "./datasource/sdfiles/DrugBank_FDA_Approved_1-144.sdf")
        )
        builder = tb.TableBuilder()
        flt = tf.GlsDistMatrixFilter(
            range(144),
            sup,
            0.1, 8, 40, 100
        )
        builder.add_filter(flt)
        data = {}
        flt.write(data)
        worker = tb.RowWiseWorker(
            zip(*builder.args_generators), tb.filter_cascade,
            builder.data
        )
        yield worker.run()

    @unittest.skip("")
    @tt.gen_test(timeout=120)
    def test_graph_performance2(self):
        import asyncio
        import itertools
        import functools
        from concurrent import futures as cf
        sup = map(
            lambda x: json.dumps(x.jsonized()),
            v2000reader.mols_from_file(
                "./datasource/sdfiles/DrugBank_FDA_Approved_1-144.sdf")
        )
        builder = tb.TableBuilder()
        flt = tf.GlsDistMatrixFilter(
            range(144),
            sup,
            0.1, 8, 40, 100
        )
        builder.add_filter(flt)
        loop = asyncio.get_event_loop()
        async def task():
            exec_ = cf.ProcessPoolExecutor(4)
            futs = []
            for pair in itertools.combinations(flt.arrays, 2):
                futs.append(loop.run_in_executor(
                    exec_,
                    functools.partial(
                        tb.filter_cascade, builder.row_processors, (pair,))
                ))
            for fut in futs:
                res = await fut
                if res is not None:
                    print(res)

        loop.run_until_complete(task())
        loop.close()


if __name__ == '__main__':
    unittest.main()
