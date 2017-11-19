#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from tornado.queues import Queue

from flashflood.lod import ListOfDict


class Edge(object):
    """Synchronous data flow edge

    Attributes:
        records (iterable): records store
        fields (lod.ListOfDict): data fields
        params (dict): optional parameters which will be sent to downstream
        task_count (int): number of tasks which should be processed
    """
    def __init__(self):
        self.records = []
        self.fields = ListOfDict()
        self.params = {}
        self.task_count = 0


class AsyncQueueEdge(object):
    """Asynchronous data flow edge

    Attributes:
        queue (tornado.queues.Queue): data queue
        status (str): edge status
        fields (lod.ListOfDict): edge status
        params (dict): optional parameters which will be sent to downstream
        task_count (int): number of tasks which should be processed
        done_count (int): number of tasks which have been processed

    Status value is either of the items below

        * ready: ready to put results
        * done: successfully the source node sent all data to the target node
        * aborted: the source node stopped sending data and remaining data
          in the edge is sent to the target node

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
        """Puts a record to the queue.

        This should be called by an upstream node."""
        yield self.queue.put(record)

    @gen.coroutine
    def get(self):
        """Gets record to the queue.

        This should be called by a downstream node."""
        res = yield self.queue.get()
        self.queue.task_done()
        return res

    @gen.coroutine
    def done(self):
        """Waits until finish sending records to its downstream node.

        This should be called by an upstream node when it is done."""
        yield self.queue.join()
        self.status = "done"

    @gen.coroutine
    def aborted(self):
        """Waits until finish sending records to its downstream node.

        This should be called by an upstream node when it is aborted."""
        yield self.queue.join()
        self.status = "aborted"
