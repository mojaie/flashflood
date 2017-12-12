#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.container import Container
from flashflood.core.task import Task
from flashflood.core.workflow import Workflow
from flashflood.lod import ListOfDict
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.transform.join import LeftJoin
from flashflood.node.writer.container import ContainerWriter


RECORDS = [
    {"id": 1, "type": "a", "value": 12.4},
    {"id": 2, "type": "b", "value": 54.23},
    {"id": 3, "type": "c", "value": 111},
    {"id": 4, "type": "a", "value": 98.0},
    {"id": 5, "type": "c", "value": None}
]

TO_BE_JOINED = [
    {"id": 1, "deadline": "12/20/2017"},
    {"id": 2, "deadline": "12/5/2017"},
    {"id": 3, "deadline": "12/5/2017"},
    {"id": 4, "deadline": "12/2/2017", "priority": 1},
    {"id": 5, "deadline": "12/18/2017"}
]


class TestJoin(AsyncTestCase):
    @gen_test
    def test_join(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        left = IterInput(RECORDS)
        right = IterInput(TO_BE_JOINED)
        join = LeftJoin("id", "id")
        wf.connect(left, join, down_port=0)
        wf.connect(right, join, down_port=1)
        wf.connect(join, ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        rcds = ListOfDict(result.records)
        self.assertEqual(len(rcds), 5)
        self.assertEqual(rcds.find("id", 4)["deadline"], "12/2/2017")
        self.assertEqual(len(rcds.find("id", 5)), 4)


if __name__ == '__main__':
    unittest.main()
