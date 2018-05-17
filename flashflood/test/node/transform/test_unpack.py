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
from flashflood.node.transform.unpack import Unpack
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


RECORDS = [
    {"id": 1, "types": ["a", "b", "c"], "value": 12.4},
    {"id": 2, "types": ["a"], "value": 54.23},
    {"id": 3, "types": ["b", "c"], "value": 111},
    {"id": 4, "types": [], "value": 98.0}
]


class TestUnpack(AsyncTestCase):
    @gen_test
    def test_unpack(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(RECORDS))
        wf.append(Unpack("types", "type"))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        rcds = ListOfDict(result.records)
        self.assertEqual(len(rcds), 6)
        self.assertEqual(rcds.find("id", 4), None)


if __name__ == '__main__':
    unittest.main()
