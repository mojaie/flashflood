#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.node import Synchronizer, Asynchronizer
from flashflood.node.function.number import Number, AsyncNumber
from flashflood.node.reader.iterator import IteratorInput


class TestNumber(AsyncTestCase):
    @gen_test
    def test_number(self):
        iter_in = IteratorInput({"value": i} for i in range(10))
        num = Number(name="num")
        num.add_in_edge(iter_in.out_edge(0), 0)
        iter_in.on_submitted()
        num.on_submitted()
        iter_in.run()
        yield num.run()
        self.assertEqual(sum(n["num"]for n in num.out_edge(0).records), 45)
        self.assertEqual(num.status, "done")

    @gen_test
    def test_asyncnumber(self):
        iter_in = IteratorInput({"value": i} for i in range(10))
        async = Asynchronizer()
        num = AsyncNumber(name="num")
        sync = Synchronizer()
        async.add_in_edge(iter_in.out_edge(0), 0)
        num.add_in_edge(async.out_edge(0), 0)
        sync.add_in_edge(num.out_edge(0), 0)
        num.interval = 0.01
        sync.interval = 0.01
        iter_in.on_submitted()
        async.on_submitted()
        num.on_submitted()
        sync.on_submitted()
        iter_in.run()
        async.run()
        num.run()
        yield sync.run()
        self.assertEqual(sum(n["num"]for n in sync.out_edge(0).records), 45)
        self.assertEqual(num.status, "done")

    @gen_test
    def test_interrupt(self):
        iter_in = IteratorInput({"value": i} for i in range(10000))
        async = Asynchronizer()
        num = AsyncNumber()
        sync = Synchronizer()
        async.add_in_edge(iter_in.out_edge(0), 0)
        num.add_in_edge(async.out_edge(0), 0)
        sync.add_in_edge(num.out_edge(0), 0)
        num.interval = 0.01
        sync.interval = 0.01
        iter_in.on_submitted()
        async.on_submitted()
        num.on_submitted()
        sync.on_submitted()
        iter_in.run()
        async.run()
        num.run()
        sync.run()
        yield async.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(sync.status, "aborted")


if __name__ == '__main__':
    unittest.main()
