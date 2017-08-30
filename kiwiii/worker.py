#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from concurrent import futures as cf
import threading

from tornado import gen, process
from tornado.queues import Queue

PROCESSES = process.cpu_count()


class Worker(object):
    """General-purpose multiprocess worker
    Args:
        args: iterable task array
        func: task processor
    """
    def __init__(self, args, func):
        self.args = args
        self.func = func
        self.interruption_requested = False
        self._queue = Queue(PROCESSES * 2)

    @gen.coroutine
    def run(self):
        self.on_start()
        with cf.ThreadPoolExecutor(PROCESSES) as tp:
            for p in range(PROCESSES):
                tp.submit(self._consumer())
            with cf.ProcessPoolExecutor(PROCESSES) as pp:
                for i, a in enumerate(self.args):
                    yield self._queue.put(pp.submit(self.func, a))
                    if self.interruption_requested:
                        yield self._queue.join()
                        self.on_interrupted()
                        return
                yield self._queue.join()
        self.status = -1
        self.on_finish()

    @gen.coroutine
    def _consumer(self):
        while True:
            f = yield self._queue.get()
            res = yield f
            with threading.Lock():
                self.on_task_done(res)
            self._queue.task_done()

    def on_task_done(self, res):
        pass

    def on_interrupted(self):
        pass

    def on_finish(self):
        pass

    def interrupt(self):
        print("Interruption requested...")
        self.interruption_requested = True


class WorkerQueue(object):
    def __init__(self):
        self.queue = Queue()
        self.current_worker_id = None
        self.current_worker = None
        self.queued_ids = []
        self.aborted_ids = []
        self._dispatcher()

    def put(self, id_, worker):
        """ Put a job to the queue """
        self.queued_ids.append(id_)
        self.queue.put_nowait((id_, worker))

    def abort(self, id_):
        if id_ in self.queued_ids:
            self.aborted_ids.append(id_)
        elif id_ == self.current_worker_id:
            self.current_worker.interrupt()

    def status(self, id_):
        if id_ in self.queued_ids:
            return "Queued"
        elif id_ == self.current_worker_id:
            return self.current_worker.status
        else:
            return "Completed"

    @gen.coroutine
    def _dispatcher(self):
        while 1:
            id_, worker = yield self.queue.get()
            self.queued_ids.remove(id_)
            if id_ in self.aborted_ids:
                self.aborted_ids.remove(id_)
                continue
            self.current_worker_id = id_
            self.current_worker = worker
            yield self.current_worker.run()
            self.current_worker_id = None
            self.current_worker = None
