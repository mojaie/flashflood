#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from multiprocessing import current_process
import unittest

from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.node import FuncNode, AsyncNode
from flashflood.core.container import Container
from flashflood.core.task import Task, InvalidOperationError
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter
from flashflood.core.workflow import Workflow, SubWorkflow


def twice(x):
    return (x * 2, current_process().name)


class TestWorkflow(AsyncTestCase):
    @gen_test
    def test_workflow(self):
        wf = Workflow()
        wf.interval = 0.01
        task = Task(wf)
        with self.assertRaises(InvalidOperationError):
            yield task.execute()
        result = Container()
        wf.append(IterInput(range(10)))
        wf.append(ContainerWriter(result))
        yield task.execute()
        self.assertEqual(sum(result.records), 45)
        self.assertTrue(all(n.status == "done" for n in wf.tasks))

    @gen_test
    def test_interrupt(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(range(10000)))
        wf.append(AsyncNode())
        wf.append(ContainerWriter(result))
        task = Task(wf)
        task.execute()
        yield task.interrupt()

    @gen_test
    def test_subworkflow(self):
        # sub
        sub = SubWorkflow(FuncNode())
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
        self.assertEqual(len(set(i[1] for i in result.records)), 1)
        self.assertEqual(sub.nodes[0].interval, 0.01)
        self.assertTrue(all(n.status == "done" for n in sub.tasks))
        self.assertTrue(all(n.status == "done" for n in wf.tasks))


if __name__ == '__main__':
    unittest.main()
