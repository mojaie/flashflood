#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from flashflood.node.io.iterator import IteratorInput
from flashflood.node.aggregate.first import AggFirst


RECORDS = [
    {"id": 1, "type": "a", "value": 12.4},
    {"id": 2, "type": "b", "value": 54.23},
    {"id": 3, "type": "c", "value": 111},
    {"id": 4, "type": "a", "value": 98.0},
    {"id": 5, "type": "c", "value": None},
    {"id": 3, "type": "a", "value": 2345}
]


class TestAggFirst(AsyncTestCase):
    @gen_test
    def test_aggfirst(self):
        iter_in = IteratorInput(RECORDS)
        stack = AggFirst('id')
        stack.add_in_edge(iter_in.out_edge(0), 0)
        iter_in.on_submitted()
        stack.on_submitted()
        self.assertEqual(stack.out_edge(0).task_count, 6)
        iter_in.run()
        yield stack.run()
        self.assertEqual(len(list(stack.out_edge(0).records)), 5)


if __name__ == '__main__':
    unittest.main()
