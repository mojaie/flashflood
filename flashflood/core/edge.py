#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from tornado.queues import Queue

from flashflood import functional
from flashflood.lod import ListOfDict


class IterEdge(object):
    """Synchronous data flow edge

    Attributes:
        records (iterable): records store
        fields (lod.ListOfDict): data fields
        params (dict): optional parameters which will be sent to downstream
    """
    def __init__(self, sampler=None):
        self.status = "ready"
        self.records = None
        self.fields = ListOfDict()
        self.params = {}
        self.sampler = sampler

    def send(self, rcds):
        if self.sampler is None:
            self.records = rcds
        else:
            self.records = list(rcds)
            self.sampler.put_from_list(self.records)
        self.status = "done"

    def abort(self):
        self.status = "aborted"


class FuncEdge(object):
    """Function data flow edge

    Attributes:
        func (callable): function to be applied
        records (iterable): records store
        fields (lod.ListOfDict): data fields
        params (dict): optional parameters which will be sent to downstream
    """
    def __init__(self, sampler=None):
        self.status = "ready"
        self.func = None
        self.records = None
        self.fields = ListOfDict()
        self.params = {}
        self.sampler = sampler

    def send(self, func, rcds):
        if self.sampler is None:
            self.func = func
            self.records = rcds
        else:
            self.func = functional.identity
            self.records = list(map(func, rcds))
            self.sampler.put_from_list(self.records)
        self.status = "done"

    def abort(self):
        self.status = "aborted"


class AsyncEdge(object):
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
    def __init__(self, sampler=None):
        self.status = "ready"
        self.queue = Queue(20)
        self.fields = ListOfDict()
        self.params = {}
        self.sampler = sampler

    @gen.coroutine
    def put(self, record):
        """Puts a record to the queue.

        This should be called by an upstream node."""
        if self.sampler is not None:
            self.sampler.put(record)
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
    def abort(self):
        """Waits until finish sending records to its downstream node.

        This should be called by an upstream node when it is aborted."""
        yield self.queue.join()
        self.status = "aborted"
