#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.node import Asynchronizer
from flashflood.node.io.iterator import IteratorInput
from flashflood.node.io.json import JSONResponse, AsyncJSONResponse
from flashflood.core.workflow import Workflow


def twice(dict_):
    return {"value": dict_["value"] * 2}


class TestWorkflow(AsyncTestCase):
    @gen_test
    def test_workflow(self):
        wf = Workflow()
        iter_in = IteratorInput(range(10))
        res = JSONResponse(wf)
        wf.connect(iter_in, res)
        yield wf.submit()
        self.assertEqual(sum(wf.result_records), 45)
        self.assertTrue(all(n.status == "done" for n in wf.nodes))

    @gen_test
    def test_asyncworkflow(self):
        wf = Workflow()
        wf.interval = 0.01
        iter_in = IteratorInput(range(10000))
        async = Asynchronizer()
        res = AsyncJSONResponse(wf)
        res.interval = 0.01
        wf.connect(iter_in, async)
        wf.connect(async, res)
        wf.on_submitted()
        self.assertEqual(iter_in.status, "ready")
        self.assertEqual(async.status, "ready")
        self.assertEqual(res.status, "ready")
        wf.run()
        self.assertEqual(iter_in.status, "done")
        self.assertEqual(async.status, "running")
        self.assertEqual(res.status, "running")
        yield wf.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(iter_in.status, "done")
        self.assertEqual(async.status, "aborted")
        self.assertEqual(res.status, "aborted")
        self.assertEqual(wf.task_count, 10000)
        self.assertGreater(wf.done_count, 0)


if __name__ == '__main__':
    unittest.main()
