#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools
import unittest

from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.container import Container
from flashflood.core.task import Task
from flashflood.core.workflow import Workflow
from flashflood.node.field.number import Number, AsyncNumber
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


class TestNumber(AsyncTestCase):
    @gen_test
    def test_number(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput({"value": i} for i in range(10)))
        wf.append(Number("num"))
        cnt2 = functools.partial(itertools.count, start=2, step=2)
        wf.append(AsyncNumber("num2", counter=cnt2))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        self.assertEqual(sum(n["num"]for n in result.records), 45)
        self.assertEqual(sum(n["num2"]for n in result.records), 110)


if __name__ == '__main__':
    unittest.main()
