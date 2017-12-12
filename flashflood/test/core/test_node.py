#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.container import Container
from flashflood.core.node import IterNode, FuncNode, AsyncNode
from flashflood.core.task import Task
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


def twice(x):
    return x * 2


class TestNode(AsyncTestCase):
    @gen_test
    def test_iternode(self):
        container = Container()
        iter_in = IterInput(range(10))
        itert = Task(iter_in)
        mid = IterNode()
        midt = Task(mid)
        out = ContainerWriter(container)
        outt = Task(out)
        [setattr(n, "interval", 0.01) for n in (iter_in, mid, out)]
        mid.add_in_edge(iter_in.out_edge(0), 0)
        out.add_in_edge(mid.out_edge(0), 0)
        itert.on_submit()
        midt.on_submit()
        outt.on_submit()
        itert.run()
        midt.run()
        yield outt.run()
        self.assertEqual(sum(container.records), 45)
        self.assertEqual(itert.status, "done")
        self.assertEqual(midt.status, "done")
        self.assertEqual(outt.status, "done")

    @gen_test
    def test_funcnode(self):
        container = Container()
        iter_in = IterInput(range(10))
        itert = Task(iter_in)
        mid = FuncNode(twice)
        midt = Task(mid)
        mid2 = FuncNode(twice)
        mid2t = Task(mid2)
        out = ContainerWriter(container)
        outt = Task(out)
        [setattr(n, "interval", 0.01) for n in (iter_in, mid, mid2, out)]
        mid.add_in_edge(iter_in.out_edge(0), 0)
        mid2.add_in_edge(mid.out_edge(0), 0)
        out.add_in_edge(mid2.out_edge(0), 0)
        itert.on_submit()
        midt.on_submit()
        mid2t.on_submit()
        outt.on_submit()
        itert.run()
        midt.run()
        mid2t.run()
        yield outt.run()
        self.assertEqual(sum(container.records), 180)
        self.assertEqual(itert.status, "done")
        self.assertEqual(midt.status, "done")
        self.assertEqual(mid2t.status, "done")
        self.assertEqual(outt.status, "done")

    @gen_test
    def test_asyncnode(self):
        container = Container()
        iter_in = IterInput(range(10))
        itert = Task(iter_in)
        mid = AsyncNode()
        midt = Task(mid)
        mid2 = AsyncNode()
        mid2t = Task(mid2)
        out = ContainerWriter(container)
        outt = Task(out)
        [setattr(n, "interval", 0.01) for n in (iter_in, mid, mid2, out)]
        mid.add_in_edge(iter_in.out_edge(0), 0)
        mid2.add_in_edge(mid.out_edge(0), 0)
        out.add_in_edge(mid2.out_edge(0), 0)
        itert.on_submit()
        midt.on_submit()
        mid2t.on_submit()
        outt.on_submit()
        itert.run()
        midt.run()
        mid2t.run()
        yield outt.run()
        self.assertEqual(sum(container.records), 45)
        self.assertEqual(itert.status, "done")
        self.assertEqual(midt.status, "done")
        self.assertEqual(mid2t.status, "done")
        self.assertEqual(outt.status, "done")

    @gen_test
    def test_func_to_async(self):
        container = Container()
        iter_in = IterInput(range(10))
        itert = Task(iter_in)
        mid = FuncNode(twice)
        midt = Task(mid)
        mid2 = AsyncNode()
        mid2t = Task(mid2)
        mid3 = FuncNode(twice)
        mid3t = Task(mid3)
        out = ContainerWriter(container)
        outt = Task(out)
        [setattr(n, "interval", 0.01) for n in (iter_in, mid, mid2, mid3, out)]
        mid.add_in_edge(iter_in.out_edge(0), 0)
        mid2.add_in_edge(mid.out_edge(0), 0)
        mid3.add_in_edge(mid2.out_edge(0), 0)
        out.add_in_edge(mid3.out_edge(0), 0)
        itert.on_submit()
        midt.on_submit()
        mid2t.on_submit()
        mid3t.on_submit()
        outt.on_submit()
        itert.run()
        midt.run()
        mid2t.run()
        mid3t.run()
        yield outt.run()
        self.assertEqual(sum(container.records), 180)
        self.assertEqual(itert.status, "done")
        self.assertEqual(midt.status, "done")
        self.assertEqual(mid2t.status, "done")
        self.assertEqual(mid3t.status, "done")
        self.assertEqual(outt.status, "done")

    @gen_test
    def test_interrupt(self):
        container = Container()
        iter_in = IterInput(range(10000))
        itert = Task(iter_in)
        mid = AsyncNode()
        midt = Task(mid)
        mid2 = AsyncNode()
        mid2t = Task(mid2)
        out = ContainerWriter(container)
        outt = Task(out)
        [setattr(n, "interval", 0.01) for n in (iter_in, mid, mid2, out)]
        mid.add_in_edge(iter_in.out_edge(0), 0)
        mid2.add_in_edge(mid.out_edge(0), 0)
        out.add_in_edge(mid2.out_edge(0), 0)
        itert.on_submit()
        midt.on_submit()
        mid2t.on_submit()
        outt.on_submit()
        itert.run()
        midt.run()
        mid2t.run()
        outt.run()
        midt.interrupt()
        while outt.status != "aborted":
            yield gen.sleep(0.01)
        self.assertEqual(itert.status, "done")
        self.assertEqual(midt.status, "aborted")
        self.assertEqual(mid2t.status, "aborted")
        self.assertEqual(outt.status, "aborted")


if __name__ == '__main__':
    unittest.main()
