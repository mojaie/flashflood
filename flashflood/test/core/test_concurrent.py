#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from multiprocessing import current_process
import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from flashflood import static
from flashflood.core.concurrent import ConcurrentSubWorkflow
from flashflood.core.container import Container
from flashflood.core.node import FunctionNode
from flashflood.core.workflow import Workflow
from flashflood.node.reader.iterator import IteratorInput
from flashflood.node.writer.container import AsyncContainerWriter


def twice(x):
    return (x * 2, current_process().name)


class TestConcurrentSubWorkflow(AsyncTestCase):
    @gen_test
    def test_concurrent(self):
        # sub
        sub = ConcurrentSubWorkflow()
        tw = FunctionNode(twice)
        sub.set_entrance(tw)
        sub.set_exit(tw)
        # main
        wf = Workflow()
        results = Container()
        iter_in = IteratorInput(range(10))
        writer = AsyncContainerWriter(results)
        wf.append(iter_in)
        wf.append(sub)
        wf.append(writer)
        yield wf.submit()
        self.assertEqual(sum(i[0] for i in results.records), 90)
        self.assertEqual(
            len(set(i[1] for i in results.records)), static.PROCESSES)
        self.assertTrue(all(n.status == "done" for n in sub.nodes))
        self.assertTrue(all(n.status == "done" for n in wf.nodes))

    @gen_test
    def test_concurrent_interrupt(self):
        # sub
        sub = ConcurrentSubWorkflow()
        tw = FunctionNode(twice)
        sub.set_entrance(tw)
        sub.set_exit(tw)
        # main
        wf = Workflow()
        results = Container()
        iter_in = IteratorInput(range(10000))
        writer = AsyncContainerWriter(results)
        wf.append(iter_in)
        wf.append(sub)
        wf.append(writer)
        wf.interval = 0.01
        wf.submit()
        yield wf.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(wf.status, "aborted")


if __name__ == '__main__':
    unittest.main()
