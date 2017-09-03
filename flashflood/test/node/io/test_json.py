#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from flashflood.node.io.json import JSONResponse, AsyncJSONResponse
from flashflood.node.io.iterator import IteratorInput
from flashflood.core.node import Asynchronizer
from flashflood.core.workflow import Workflow


class TestJSONResponse(AsyncTestCase):
    @gen_test
    def test_jsonresponse(self):
        wf = Workflow()
        iter_in = IteratorInput({"value": i} for i in range(10))
        res = JSONResponse(wf)
        wf.connect(iter_in, res)
        yield wf.submit()
        data = wf.output()
        self.assertEqual(len(data["records"]), 10)
        self.assertEqual(data["status"], "done")

    @gen_test
    def test_asyncjsonresponse(self):
        wf = Workflow()
        iter_in = IteratorInput({"value": i} for i in range(10))
        async = Asynchronizer()
        res = AsyncJSONResponse(wf)
        wf.connect(iter_in, async)
        wf.connect(async, res)
        res.interval = 0.01
        yield wf.submit()
        data = wf.output()
        self.assertEqual(len(data["records"]), 10)
        self.assertEqual(data["status"], "done")

    @gen_test
    def test_interrupt(self):
        wf = Workflow()
        iter_in = IteratorInput({"value": i} for i in range(10000))
        async = Asynchronizer()
        res = AsyncJSONResponse(wf)
        wf.connect(iter_in, async)
        wf.connect(async, res)
        res.interval = 0.01
        wf.submit()
        yield wf.interrupt()
        yield gen.sleep(0.1)
        data = wf.output()
        self.assertEqual(data["status"], "aborted")
        self.assertGreater(len(data["records"]), 0)


if __name__ == '__main__':
    unittest.main()
