#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.container import Container
from flashflood.core.node import IterNode, FuncNode, AsyncNode
from flashflood.core.task import Task
from flashflood.core.workflow import Workflow
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.record.merge import MergeRecords
from flashflood.node.writer.container import ContainerWriter


RECORDS1 = [
    {"id": 1, "type": "a", "value": 12.4},
    {"id": 2, "type": "b", "value": 54.23}
]

RECORDS2 = [
    {"id": 3, "type": "c", "value": 111},
    {"id": 4, "type": "a", "value": 98.0}
]
RECORDS3 = [
    {"id": 5, "type": "c", "value": None},
    {"id": 6, "type": "d", "value": 0.125}
]


class TestMerge(AsyncTestCase):
    @gen_test
    def test_merge(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        in1 = AsyncNode()
        wf.connect(IterInput(RECORDS1), in1)
        in2 = FuncNode()
        wf.connect(IterInput(RECORDS2), in2)
        in3 = IterNode()
        wf.connect(IterInput(RECORDS3), in3)
        merge = MergeRecords()
        wf.connect(in1, merge)
        wf.connect(in2, merge)
        wf.connect(in3, merge)
        wf.connect(merge, ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        self.assertEqual(len(list(result.records)), 6)


if __name__ == '__main__':
    unittest.main()
