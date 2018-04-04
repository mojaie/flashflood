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
from flashflood.node.field.extract import Extract
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


class TestExtract(AsyncTestCase):
    @gen_test
    def test_extract(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(
            {"idx": i, "double": {"value": {"v": i * 2}}} for i in range(10)
        ))
        wf.append(Extract("double", ["value.v"], in_place=True))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        self.assertEqual(sum(i["value.v"] for i in result.records), 90)


if __name__ == '__main__':
    unittest.main()
