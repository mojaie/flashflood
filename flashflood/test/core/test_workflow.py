#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.node import Asynchronizer
from flashflood.core.container import Container
from flashflood.node.io.iterator import IteratorInput
from flashflood.node.writer.container import (
    ContainerWriter, AsyncContainerWriter)
from flashflood.core.workflow import Workflow


def twice(dict_):
    return {"value": dict_["value"] * 2}


class TestWorkflow(AsyncTestCase):
    @gen_test
    def test_workflow(self):
        wf = Workflow()
        wf.results = Container()
        iter_in = IteratorInput(range(10))
        writer = ContainerWriter(wf.results)
        wf.connect(iter_in, writer)
        yield wf.submit()
        self.assertEqual(sum(wf.results.records), 45)
        self.assertTrue(all(n.status == "done" for n in wf.nodes))

    @gen_test
    def test_asyncworkflow(self):
        wf = Workflow()
        wf.results = Container()
        wf.interval = 0.01
        iter_in = IteratorInput(range(10000))
        async = Asynchronizer()
        writer = AsyncContainerWriter(wf.results)
        writer.interval = 0.01
        wf.connect(iter_in, async)
        wf.connect(async, writer)
        wf.on_submitted()
        self.assertEqual(iter_in.status, "ready")
        self.assertEqual(async.status, "ready")
        self.assertEqual(writer.status, "ready")
        wf.run()
        self.assertEqual(iter_in.status, "done")
        self.assertEqual(async.status, "running")
        self.assertEqual(writer.status, "running")
        yield wf.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(iter_in.status, "done")
        self.assertEqual(async.status, "aborted")
        self.assertEqual(writer.status, "aborted")


if __name__ == '__main__':
    unittest.main()
