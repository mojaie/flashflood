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
from flashflood.node.transform.unstack import Unstack
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


RECORDS = [
    {"id": 1, "type": "a", "value": 12.4},
    {"id": 2, "type": "b", "value": 54.23},
    {"id": 3, "type": "c", "value": 111},
    {"id": 4, "type": "a", "value": 98.0},
    {"id": 5, "type": "c", "value": None},
    {"id": 3, "type": "a", "value": 2345},
    {"id": 5, "type": "c", "value": None, "dup": True}
]


class TestUnstack(AsyncTestCase):
    @gen_test
    def test_unstack(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(RECORDS))
        wf.append(Unstack(['id'], "type", "value"))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        rcds = ListOfDict(result.records)
        self.assertEqual(len(rcds), 5)
        self.assertEqual(rcds.find("id", 3)["a"], 2345)
        self.assertEqual(len(rcds.find("id", 5)), 3)


if __name__ == '__main__':
    unittest.main()
