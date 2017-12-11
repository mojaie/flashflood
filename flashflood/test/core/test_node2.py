#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.container import Container
from flashflood.core.node2 import IterNode, FuncNode, AsyncNode
from flashflood.node.reader.iterator2 import IteratorInput
from flashflood.node.writer.container2 import ContainerWriter


def twice(x):
    return x * 2


class TestNode(AsyncTestCase):
    @gen_test
    def test_iternode(self):
        container = Container()
        iter_in = IteratorInput(range(10))
        mid = IterNode()
        out = ContainerWriter(container)
        mid.add_in_edge(iter_in.out_edge(0), 0)
        out.add_in_edge(mid.out_edge(0), 0)
        iter_in.on_submitted()
        mid.on_submitted()
        out.on_submitted()
        iter_in.run()
        mid.run()
        yield out.run()
        self.assertEqual(sum(container.records), 45)
        self.assertEqual(iter_in.status, "done")
        self.assertEqual(mid.status, "done")
        self.assertEqual(out.status, "done")

    @gen_test
    def test_funcnode(self):
        container = Container()
        iter_in = IteratorInput(range(10))
        mid = FuncNode(twice)
        mid2 = FuncNode(twice)
        out = ContainerWriter(container)
        mid.add_in_edge(iter_in.out_edge(0), 0)
        mid2.add_in_edge(mid.out_edge(0), 0)
        out.add_in_edge(mid2.out_edge(0), 0)
        iter_in.on_submitted()
        mid.on_submitted()
        mid2.on_submitted()
        out.on_submitted()
        iter_in.run()
        mid.run()
        mid2.run()
        yield out.run()
        self.assertEqual(sum(container.records), 180)
        self.assertEqual(iter_in.status, "done")
        self.assertEqual(mid.status, "done")
        self.assertEqual(mid2.status, "done")
        self.assertEqual(out.status, "done")

    @gen_test
    def test_asyncnode(self):
        container = Container()
        iter_in = IteratorInput(range(10))
        mid = AsyncNode()
        mid2 = AsyncNode()
        out = ContainerWriter(container)
        mid2.interval = 0.01
        out.interval = 0.01
        mid.add_in_edge(iter_in.out_edge(0), 0)
        mid2.add_in_edge(mid.out_edge(0), 0)
        out.add_in_edge(mid2.out_edge(0), 0)
        iter_in.on_submitted()
        mid.on_submitted()
        mid2.on_submitted()
        out.on_submitted()
        iter_in.run()
        mid.run()
        mid2.run()
        yield out.run()
        self.assertEqual(sum(container.records), 45)
        self.assertEqual(iter_in.status, "done")
        self.assertEqual(mid.status, "done")
        self.assertEqual(mid2.status, "done")
        self.assertEqual(out.status, "done")

    @gen_test
    def test_func_to_async(self):
        container = Container()
        iter_in = IteratorInput(range(10))
        mid = FuncNode(twice)
        mid2 = AsyncNode()
        mid3 = FuncNode(twice)
        out = ContainerWriter(container)
        mid2.interval = 0.01
        mid3.interval = 0.01
        mid.add_in_edge(iter_in.out_edge(0), 0)
        mid2.add_in_edge(mid.out_edge(0), 0)
        mid3.add_in_edge(mid2.out_edge(0), 0)
        out.add_in_edge(mid3.out_edge(0), 0)
        iter_in.on_submitted()
        mid.on_submitted()
        mid2.on_submitted()
        mid3.on_submitted()
        out.on_submitted()
        iter_in.run()
        mid.run()
        mid2.run()
        mid3.run()
        out.run()
        while mid2.status != "done":  # Asynchronizer is the last
            yield gen.sleep(0.01)
        self.assertEqual(sum(container.records), 180)
        self.assertEqual(iter_in.status, "done")
        self.assertEqual(mid.status, "done")
        self.assertEqual(mid2.status, "done")
        self.assertEqual(mid3.status, "done")
        self.assertEqual(out.status, "done")

    @gen_test
    def test_interrupt(self):
        container = Container()
        iter_in = IteratorInput(range(10000))
        mid = AsyncNode()
        mid2 = AsyncNode()
        out = ContainerWriter(container)
        mid2.interval = 0.01
        out.interval = 0.01
        mid.add_in_edge(iter_in.out_edge(0), 0)
        mid2.add_in_edge(mid.out_edge(0), 0)
        out.add_in_edge(mid2.out_edge(0), 0)
        iter_in.on_submitted()
        mid.on_submitted()
        mid2.on_submitted()
        out.on_submitted()
        iter_in.run()
        mid.run()
        mid2.run()
        out.run()
        yield mid.interrupt()
        while out.status != "aborted":  # Waiting for propagation
            yield gen.sleep(0.01)
        self.assertEqual(iter_in.status, "done")
        self.assertEqual(mid.status, "aborted")
        self.assertEqual(mid2.status, "aborted")
        self.assertEqual(out.status, "aborted")


if __name__ == '__main__':
    unittest.main()
