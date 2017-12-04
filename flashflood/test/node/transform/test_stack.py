#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from flashflood.node.reader.iterator import IteratorInput
from flashflood.node.transform.stack import Stack


RECORDS = [
    {"id": 1, "type": "a", "value": 12.4},
    {"id": 2, "type": "b", "value": 54.23},
    {"id": 3, "type": "c", "value": 111},
    {"id": 4, "type": "a", "value": 98.0},
    {"id": 5, "type": "c", "value": None}
]


class TestStack(AsyncTestCase):
    @gen_test
    def test_stack(self):
        iter_in = IteratorInput(RECORDS)
        stack = Stack(('id',))
        stack.add_in_edge(iter_in.out_edge(0), 0)
        iter_in.on_submitted()
        stack.on_submitted()
        iter_in.run()
        yield stack.run()
        self.assertEqual(len(list(stack.out_edge(0).records)), 9)

    @gen_test
    def test_stack2(self):
        iter_in = IteratorInput(RECORDS)
        stack = Stack(('id',), skip_none=False)
        stack.add_in_edge(iter_in.out_edge(0), 0)
        iter_in.on_submitted()
        stack.on_submitted()

        iter_in.run()
        yield stack.run()
        self.assertEqual(len(list(stack.out_edge(0).records)), 10)


if __name__ == '__main__':
    unittest.main()
