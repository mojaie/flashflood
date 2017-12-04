#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.node import Synchronizer, Asynchronizer
from flashflood.node.io.iterator import IteratorInput


class TestNode(AsyncTestCase):
    @gen_test
    def test_synchronizer(self):
        iter_in = IteratorInput({"value": i} for i in range(10))
        async = Asynchronizer()
        sync = Synchronizer()
        async.add_in_edge(iter_in.out_edge(0), 0)
        sync.add_in_edge(async.out_edge(0), 0)
        sync.interval = 0.01
        iter_in.on_submitted()
        async.on_submitted()
        sync.on_submitted()
        iter_in.run()
        async.run()
        yield sync.run()
        self.assertEqual(iter_in.status, "done")
        self.assertEqual(async.status, "done")
        self.assertEqual(sync.status, "done")

    @gen_test
    def test_interrupt(self):
        iter_in = IteratorInput(range(10000))
        async = Asynchronizer()
        sync = Synchronizer()
        async.add_in_edge(iter_in.out_edge(0), 0)
        sync.add_in_edge(async.out_edge(0), 0)
        sync.interval = 0.01
        iter_in.on_submitted()
        async.on_submitted()
        sync.on_submitted()
        iter_in.run()
        async.run()
        sync.run()
        yield async.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(sync.status, "aborted")


if __name__ == '__main__':
    unittest.main()
