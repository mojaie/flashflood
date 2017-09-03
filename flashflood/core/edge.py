#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from tornado.queues import Queue

from flashflood.lod import ListOfDict


class Edge(object):
    def __init__(self):
        self.records = []
        self.fields = ListOfDict()
        self.params = {}
        self.task_count = 0


class AsyncQueueEdge(object):
    """
    Parameters:
      status: str
        ready: ready to put results
        done: successfully the source node sent all data to the target node
        aborted: the source node stopped sending data and remaining data in the
            edge is sent to the target node
    """
    def __init__(self):
        self.queue = Queue(20)
        self.status = "ready"
        self.fields = ListOfDict()
        self.params = {}
        self.task_count = 0
        self.done_count = 0

    @gen.coroutine
    def put(self, record):
        yield self.queue.put(record)

    @gen.coroutine
    def get(self):
        res = yield self.queue.get()
        self.queue.task_done()
        return res

    @gen.coroutine
    def done(self):
        yield self.queue.join()
        self.status = "done"

    @gen.coroutine
    def aborted(self):
        yield self.queue.join()
        self.status = "aborted"
