#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.edge import AsyncQueueEdge


class TestEdge(AsyncTestCase):
    @gen_test
    def test_asyncedge(self):
        edge = AsyncQueueEdge()
        in_ = 123
        yield edge.put(in_)
        out = yield edge.get()
        self.assertEqual(out, 123)
        yield edge.done()
        self.assertEqual(edge.status, "done")


if __name__ == '__main__':
    unittest.main()
