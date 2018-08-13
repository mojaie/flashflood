#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time

from tornado import gen
from tornado.queues import Queue


class JobQueue(object):
    def __init__(self):
        self.queue = Queue(20)
        self.alive = {}
        self._dispatcher()

    @gen.coroutine
    def put(self, task, now=time.time()):
        """ Put a job to the queue """
        self.alive[task.id] = task
        task.on_submit()
        yield self.queue.put(task)

    def get(self, id_):
        try:
            return self.alive[id_]
        except KeyError:
            raise ValueError('Task {} not found'.format(id_))

    def abort(self, id_):
        task = self.get(id_)
        if task.status == "running":
            task.interrupt()
        if task.status == "ready":
            task.status = "cancelled"

    @gen.coroutine
    def _dispatcher(self):
        while 1:
            task = yield self.queue.get()
            if task.status == "cancelled":
                continue
            yield task.run()
            del self.alive[task.id]

    def tasks_iter(self):
        return self.alive.values()
