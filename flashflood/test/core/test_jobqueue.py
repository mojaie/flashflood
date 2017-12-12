#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time
import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from flashflood.core.jobqueue import JobQueue
from flashflood.core.node import Node
from flashflood.core.task import Task


class EagerNode(Node):
    def run(self, on_finish, on_abort):
        on_finish()

    def on_submit(self):
        pass


class LazyNode(Node):
    """For async task test"""
    def __init__(self, **kwargs):
        self._interrupted = False

    @gen.coroutine
    def run(self, on_finish, on_abort):
        while not self._interrupted:
            yield gen.sleep(0.2)
        on_abort()

    def on_submit(self):
        pass

    def interrupt(self):
        self._interrupted = True


class TestJobQueue(AsyncTestCase):
    @gen_test
    def test_store(self):
        jq = JobQueue()
        task1 = Task(EagerNode())
        task2 = Task(EagerNode())
        runner3 = Task(EagerNode())
        yield jq.put(task1)
        yield jq.put(task2)
        runner3.creation_time = time.time() - jq.task_lifetime - 100
        yield jq.put(runner3)
        self.assertEqual(len(jq.store), 2)
        self.assertEqual(jq.get(task2.id).id, task2.id)
        with self.assertRaises(ValueError):
            jq.get("invalidKey")

    @gen_test
    def test_queue(self):
        jq = JobQueue()
        task1 = Task(LazyNode())
        task2 = Task(LazyNode())
        yield jq.put(task1)
        yield jq.put(task2)
        while task1.status != "running":
            yield gen.sleep(0.01)
        self.assertEqual(task2.status, "ready")
        yield jq.abort(task1.id)
        while task1.status != "aborted":
            yield gen.sleep(0.01)
        self.assertEqual(task2.status, "running")
        jq.abort(task2.id)
        while task2.status != "aborted":
            yield gen.sleep(0.01)
        self.assertEqual(len(jq.store), 2)


if __name__ == '__main__':
    unittest.main()
