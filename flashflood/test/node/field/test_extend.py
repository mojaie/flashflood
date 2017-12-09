#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import pickle
import unittest

from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.container import Container
from flashflood.node.field.extend import Extend
from flashflood.node.reader.iterator import IteratorInput
from flashflood.node.writer.container import ContainerWriter


class TestExtend(AsyncTestCase):
    @gen_test
    def test_copy(self):
        result = Container()
        iter_in = IteratorInput({"idx": i} for i in range(10))
        ext = Extend("copied", "idx")
        self.assertIsInstance(pickle.dumps(ext.func), bytes)
        out = ContainerWriter(result)
        ext.add_in_edge(iter_in.out_edge(0), 0)
        out.add_in_edge(ext.out_edge(0), 0)
        iter_in.on_submitted()
        ext.on_submitted()
        out.on_submitted()
        iter_in.run()
        ext.run()
        yield out.run()
        self.assertEqual(sum(i["idx"] for i in result.records), 45)
        self.assertEqual(sum(i["copied"] for i in result.records), 45)

    @gen_test
    def test_replace(self):
        result = Container()
        iter_in = IteratorInput({"idx": i} for i in range(10))
        ext = Extend("new_idx", "idx", in_place=True)
        self.assertIsInstance(pickle.dumps(ext.func), bytes)
        out = ContainerWriter(result)
        ext.add_in_edge(iter_in.out_edge(0), 0)
        out.add_in_edge(ext.out_edge(0), 0)
        iter_in.on_submitted()
        ext.on_submitted()
        out.on_submitted()
        iter_in.run()
        ext.run()
        yield out.run()
        self.assertFalse("idx" in result.records[0])
        self.assertEqual(sum(i["new_idx"] for i in result.records), 45)


if __name__ == '__main__':
    unittest.main()
