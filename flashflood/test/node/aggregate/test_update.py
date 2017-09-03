#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from flashflood.lod import ListOfDict
from flashflood.node.io.iterator import IteratorInput
from flashflood.node.aggregate.update import AggUpdate


RECORDS = [
    {"id": 1, "type": "a", "value": 12.4},
    {"id": 2, "type": "b", "value": 54.23},
    {"id": 3, "type": "c", "value": 111},
    {"id": 4, "type": "a", "value": 98.0},
    {"id": 5, "type": "c", "value": None},
    {"id": 3, "type": "a", "value": 2345},
    {"id": 5, "type": "c", "value": None, "dup": True}
]


class TestAggUpdate(AsyncTestCase):
    @gen_test
    def test_aggupdate(self):
        iter_in = IteratorInput(RECORDS)
        stack = AggUpdate('id')
        stack.add_in_edge(iter_in.out_edge(0), 0)
        iter_in.on_submitted()
        stack.on_submitted()
        self.assertEqual(stack.out_edge(0).task_count, 7)
        iter_in.run()
        yield stack.run()
        rcds = ListOfDict(stack.out_edge(0).records)
        self.assertEqual(len(rcds), 5)
        self.assertEqual(rcds.find("id", 3)["value"], 2345)
        self.assertEqual(len(rcds.find("id", 5)), 4)


if __name__ == '__main__':
    unittest.main()
