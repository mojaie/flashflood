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
import flashflood.node as nd


RECORDS = [
    {"id": 1, "type": "a", "value": 2},
    {"id": 2, "type": "b", "value": 4},
    {"id": 3, "type": "c", "value": 6},
    {"id": 4, "type": "a", "value": 8},
    {"id": 5, "type": "c", "value": 10}
]


def twice(x):
    return x * 2


def halve(x):
    return x / 2


class TestReplicate(AsyncTestCase):
    @gen_test
    def test_replicate(self):
        wf = Workflow()
        wf.interval = 0.01
        result1 = Container()
        result2 = Container()
        wf.append(nd.IterInput(RECORDS))
        rep = nd.Replicate(2)
        wf.append(rep)
        # twice
        wf.connect(rep, nd.Extend("twiced", "value", func=twice), up_port=0)
        wf.append(nd.ContainerWriter(result1))
        # halve
        wf.connect(rep, nd.Extend("halved", "value", func=halve), up_port=1)
        wf.append(nd.ContainerWriter(result2))
        task = Task(wf)
        yield task.execute()
        self.assertEqual(sum(r["twiced"] for r in result1.records), 60)
        self.assertEqual(sum(r["halved"] for r in result2.records), 15)


if __name__ == '__main__':
    unittest.main()
