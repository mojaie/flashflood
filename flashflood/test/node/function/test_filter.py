#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from flashflood.node.function.filter import Filter, MPFilter
from flashflood.node.io.iterator import IteratorInput
from flashflood.core.node import Synchronizer


def f(x):
    if x % 2:
        return x


class TestFilter(AsyncTestCase):
    @gen_test
    def test_filter(self):
        iter_in = IteratorInput(range(10))
        filter_ = Filter(f)
        filter_.add_in_edge(iter_in.out_edge(0), 0)
        iter_in.on_submitted()
        filter_.on_submitted()
        iter_in.run()
        yield filter_.run()
        self.assertEqual(sum(filter_.out_edge(0).records), 25)
        self.assertEqual(filter_.status, "done")

    @gen_test
    def test_mpfilter(self):
        iter_in = IteratorInput(range(10))
        mpf = MPFilter(f)
        sync = Synchronizer()
        mpf.add_in_edge(iter_in.out_edge(0), 0)
        sync.add_in_edge(mpf.out_edge(0), 0)
        mpf.interval = 0.01
        iter_in.on_submitted()
        mpf.on_submitted()
        sync.on_submitted()
        iter_in.run()
        mpf.run()
        yield sync.run()
        self.assertEqual(iter_in.status, "done")
        self.assertEqual(mpf.status, "done")
        self.assertEqual(sync.status, "done")
        self.assertEqual(sum(sync.out_edge(0).records), 25)

    @gen_test
    def test_interrupt(self):
        iter_in = IteratorInput(range(10000))
        mpf = MPFilter(f)
        sync = Synchronizer()
        mpf.add_in_edge(iter_in.out_edge(0), 0)
        sync.add_in_edge(mpf.out_edge(0), 0)
        sync.interval = 0.01
        iter_in.on_submitted()
        mpf.on_submitted()
        sync.on_submitted()
        iter_in.run()
        mpf.run()
        sync.run()
        yield mpf.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(sync.status, "aborted")


if __name__ == '__main__':
    unittest.main()
