#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest
import yaml

from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.container import Container
from flashflood.core.task import Task
from flashflood.core.workflow import Workflow
from flashflood.node.reader.httpfetch import HttpFetchInput
from flashflood.node.writer.container import ContainerWriter


def parser(response):
    data = yaml.load(response)
    return data["requirements"]["run"]


class TestHttpFetch(AsyncTestCase):
    @gen_test
    def test_httpfetch(self):
        wf = Workflow()
        wf.interval = 0.01
        result = Container()
        url = "https://raw.githubusercontent.com/mojaie/flashflood/master/conda-recipe/meta.yaml"
        wf.append(HttpFetchInput(url, response_parser=parser))
        wf.append(ContainerWriter(result))
        task = Task(wf)
        yield task.execute()
        self.assertEqual(len(result.records), 4)


if __name__ == '__main__':
    unittest.main()
