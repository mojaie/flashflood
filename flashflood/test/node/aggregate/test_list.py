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
from flashflood.node.aggregate.list import AggList
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


RECORDS = [
    {"id": 1, "type": "a", "value": "nice"},
    {"id": 2, "type": "b", "value": "nice"},
    {"id": 3, "type": "c", "value": "good"},
    {"id": 4, "type": "a", "value": "good"},
    {"id": 5, "type": "c", "value": "excellent"},
    {"id": 3, "type": "a", "value": "splendid"},
    {"id": 5, "type": "c", "value": "good"}
]


class TestAggList(AsyncTestCase):
    @gen_test
    def test_agglist(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(RECORDS))
        wf.append(AggList("type", "value"))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        rcds = ListOfDict(result.records)
        self.assertEqual(len(rcds), 3)
        self.assertEqual(len(rcds.find("type", "c")["list"]), 3)


if __name__ == '__main__':
    unittest.main()
