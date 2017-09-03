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
        self.store = []
        self.task_lifetime = 86400 * 7  # Time(sec)
        self._dispatcher()

    def __len__(self):
        return len(self.store)

    @gen.coroutine
    def put(self, task, now=time.time()):
        """ Put a job to the queue """
        self.store.append(task)
        # remove expired data
        alive = []
        for task in self.store:
            if task.creation_time + self.task_lifetime > now:
                alive.append(task)
        self.store = alive
        yield self.queue.put(task)
        task.on_submitted()

    def get(self, id_):
        for task in self.store:
            if task.id == id_:
                return task
        raise ValueError('Task {} not found'.format(id_[:8]))

    @gen.coroutine
    def abort(self, id_):
        task = self.get(id_)
        if task.status == "running":
            yield task.interrupt()
        if task.status == "ready":
            task.status = "cancelled"

    @gen.coroutine
    def _dispatcher(self):
        while 1:
            task = yield self.queue.get()
            if task.status == "cancelled":
                continue
            yield task.run()

    def tasks_iter(self):
        for task in self.store:
            expires = task.creation_time + self.task_lifetime
            yield task, expires
