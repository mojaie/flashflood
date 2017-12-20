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
from flashflood.node.field.remove import RemoveField
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


class TestRemove(AsyncTestCase):
    @gen_test
    def test_remove(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput({"idx": i, "idx2": i} for i in range(10)))
        wf.append(RemoveField("idx2"))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        self.assertEqual(sum(len(i) for i in result.records), 10)
        self.assertFalse("idx2" in result.records[0])


if __name__ == '__main__':
    unittest.main()
