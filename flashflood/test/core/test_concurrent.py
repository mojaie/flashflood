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
from flashflood.core.concurrent import ConcurrentNode, ConcurrentFilter
from flashflood.core.container import Container
from flashflood.core.node import FuncNode
from flashflood.core.task import Task
from flashflood.core.workflow import Workflow, SubWorkflow
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


def twice(x):
    return (x * 2, current_process().name)


def odd(x):
    if x % 2:
        return (x, current_process().name)


class TestConcurrentNode(AsyncTestCase):
    @gen_test
    def test_concurrent(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(range(10)))
        wf.append(ConcurrentNode(func=twice))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        self.assertEqual(sum(i[0] for i in result.records), 90)
        self.assertEqual(
            len(set(i[1] for i in result.records)), static.PROCESSES)
        self.assertTrue(all(n.status == "done" for n in wf.tasks))

    @gen_test
    def test_filter(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(range(10)))
        wf.append(ConcurrentFilter(func=odd))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        self.assertEqual(sum(i[0] for i in result.records), 25)
        self.assertEqual(
            len(set(i[1] for i in result.records)), static.PROCESSES)
        self.assertTrue(all(n.status == "done" for n in wf.tasks))

    @gen_test
    def test_interrupt(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(range(10000)))
        wf.append(ConcurrentNode(func=twice))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        task.execute()
        task.interrupt()
        while task.status != "aborted":
            yield gen.sleep(0.01)

    @gen_test
    def test_subworkflow(self):
        # sub
        sub = SubWorkflow(ConcurrentNode())
        tw = FuncNode(twice)
        sub.set_entrance(tw)
        sub.set_exit(tw)
        # main
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(range(10)))
        wf.append(sub)
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        self.assertEqual(sum(i[0] for i in result.records), 90)
        self.assertEqual(len(set(i[1] for i in result.records)), 4)
        self.assertEqual(sub.nodes[0].interval, 0.01)
        self.assertTrue(all(n.status == "done" for n in sub.tasks))
        self.assertTrue(all(n.status == "done" for n in wf.tasks))


if __name__ == '__main__':
    unittest.main()
