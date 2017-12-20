#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from flashflood.lod import ListOfDict
from flashflood.core.container import Container
from flashflood.core.task import Task
from flashflood.core.workflow import Workflow
from flashflood.node.aggregate.sum import AggSum
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


RECORDS = [
    {"id": 1, "type": "a", "value": 12.4},
    {"id": 2, "type": "b", "value": 54.23},
    {"id": 3, "type": "c", "value": 111},
    {"id": 4, "type": "a", "value": 98.0},
    {"id": 5, "type": "c", "value": 8},
    {"id": 3, "type": "a", "value": 2345},
    {"id": 5, "type": "c", "value": 0.6, "dup": True}
]


class TestAggUpdate(AsyncTestCase):
    @gen_test
    def test_aggsum(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(RECORDS))
        wf.append(AggSum("type", "value"))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        rcds = ListOfDict(result.records)
        self.assertEqual(len(rcds), 3)
        self.assertEqual(rcds.find("type", "a")["sum"], 2455.4)
        self.assertFalse("value" in rcds.find("type", "a"))


if __name__ == '__main__':
    unittest.main()
