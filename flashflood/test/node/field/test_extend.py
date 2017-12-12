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
from flashflood.node.field.extend import Extend
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


class TestExtend(AsyncTestCase):
    @gen_test
    def test_copy(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput({"idx": i} for i in range(10)))
        wf.append(Extend("copied", "idx"))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        self.assertEqual(sum(i["idx"] for i in result.records), 45)
        self.assertEqual(sum(i["copied"] for i in result.records), 45)

    @gen_test
    def test_replace(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput({"idx": i} for i in range(10)))
        wf.append(Extend("new_idx", "idx", in_place=True))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        self.assertFalse("idx" in result.records[0])
        self.assertEqual(sum(i["new_idx"] for i in result.records), 45)


if __name__ == '__main__':
    unittest.main()
