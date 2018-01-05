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
from flashflood.node.control.filter import Filter
from flashflood.node.reader.iterinput import IterInput
from flashflood.node.writer.container import ContainerWriter


def f(x):
    return x % 2


class TestFilter(AsyncTestCase):
    @gen_test
    def test_filter(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        wf.append(IterInput(range(10)))
        wf.append(Filter(f))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        self.assertEqual(sum(result.records), 25)


if __name__ == '__main__':
    unittest.main()
