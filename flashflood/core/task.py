#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from concurrent import futures as cf
import threading
import time
import uuid

from tornado import gen
from tornado.queues import Queue

from flashflood import static
from flashflood.lod import ListOfDict
from flashflood.util import debug


class Task(object):
    """
    Parameters:
        status: str
            ready, running, done, aborted
            interrupted: interrupted but the task is not yet aborted
            cancelled: submitted but cancelled before start
        fields: ListOfDict
            Data fields (columns)
        params: dict
            Additional parameters which would be passed to downstream nodes
    """
    def __init__(self):
        self.id = str(uuid.uuid4())
        self.status = "ready"
        self.creation_time = time.time()
        self.start_time = None
        self.fields = ListOfDict()
        self.params = {}

    def run(self):
        self.on_start()
        # do something
        self.on_finish()

    def interrupt(self):
        pass

    def on_submitted(self):
        """When the task is put into the jobqueue"""
        pass

    def submit(self):
        """Submit and run shortcut for unit testing"""
        self.on_submitted()
        self.run()

    def on_start(self):
        """When run method is called"""
        self.start_time = time.time()
        self.status = "running"

    def on_task_done(self, rcd):
        """When a record was processed by the task"""
        pass

    def on_finish(self):
        """When the task is finished without interruption"""
        self.status = "done"

    def on_aborted(self):
        """When the task is completely halted after calling interrupt method"""
        self.status = "aborted"

    def size(self):
        return debug.total_size(self)


class IdleTask(Task):
    """For async task test"""
    @gen.coroutine
    def run(self):
        self.on_start()
        while 1:
            if self.status == "interrupted":
                self.on_aborted()
                return
            yield gen.sleep(0.2)
        self.on_finish()

    @gen.coroutine
    def interrupt(self):
        self.status = "interrupted"
        while self.status != "aborted":
            yield gen.sleep(0.2)


class MPWorker(Task):
    """General-purpose multiprocess worker
    Args:
        args: iterable task array
        func: task processor
    """
    def __init__(self, args, func):
        super().__init__()
        self.args = args
        self.func = func
        self._queue = Queue(static.PROCESSES * 2)
        self.interval = 0.5

    @gen.coroutine
    def run(self):
        self.on_start()
        with cf.ThreadPoolExecutor(static.PROCESSES) as tp:
            for p in range(static.PROCESSES):
                tp.submit(self._consumer())
            with cf.ProcessPoolExecutor(static.PROCESSES) as pp:
                for i, a in enumerate(self.args):
                    yield self._queue.put(pp.submit(self.func, a))
                    if self.status == "interrupted":
                        yield self._queue.join()
                        self.on_aborted()
                        return
                yield self._queue.join()
        self.on_finish()

    @gen.coroutine
    def _consumer(self):
        while True:
            f = yield self._queue.get()
            res = yield f
            with threading.Lock():
                self.on_task_done(res)
            self._queue.task_done()

    @gen.coroutine
    def interrupt(self):
        self.status = "interrupted"
        while self.status != "aborted":
            yield gen.sleep(self.interval)


class MPWorkerResults(MPWorker):
    """For mpworker test"""
    def __init__(self, *args):
        super().__init__(*args)
        self.results = []

    def on_task_done(self, res):
        self.results.append(res)
