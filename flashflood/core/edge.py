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
        status (text): ``ready``, ``done`` or ``aborted``
        records (iterable): records store
        fields (flashflood.lod.ListOfDict): data fields
        params (dict): optional parameters which will be sent to downstream
        sampler (flashflood.core.container.Sampler): data sampler object
    """
    def __init__(self, sampler=None):
        self.status = "ready"
        self.records = None
        self.fields = ListOfDict()
        self.params = {}
        self.sampler = sampler

    def send(self, rcds):
        """Send records to the downstream.

        This will bind upstream records to IterEdge.records. Then the
        downstream node refers to the records.

        Args:
            rcds(iterable): iterator to be sent to the downstream
        """
        if self.sampler is None:
            self.records = rcds
        else:
            self.records = list(rcds)
            self.sampler.put_from_list(self.records)
        self.status = "done"

    def abort(self):
        """This will be called when the upstream abort signal was emitted.
        Abort signal will be propageted until all tasks are completely stopped.
        """
        self.status = "aborted"


class FuncEdge(object):
    """Function data flow edge

    Attributes:
        func (callable): function to be applied
        records (iterable): records store
        fields (flashflood.lod.ListOfDict): data fields
        params (dict): optional parameters which will be sent to downstream
        sampler (flashflood.core.container.Sampler): data sampler object
    """
    def __init__(self, sampler=None):
        self.status = "ready"
        self.func = None
        self.records = None
        self.fields = ListOfDict()
        self.params = {}
        self.sampler = sampler

    def send(self, func, rcds):
        """Send func and args to the downstream.

        This will bind upstream records to IterEdge.records and IterEdge.func.
        Then the downstream node refers to the records.

        Args:
            func(function): function to be applied
            rcds(iterable): iterator to be sent to the downstream
        """
        if self.sampler is None:
            self.func = func
            self.records = rcds
        else:
            self.func = functional.identity
            self.records = list(map(func, rcds))
            self.sampler.put_from_list(self.records)
        self.status = "done"

    def abort(self):
        """This will be called when the upstream abort signal was emitted.
        Abort signal will be propageted until all tasks are completely stopped.
        """
        self.status = "aborted"


class AsyncEdge(object):
    """Asynchronous data flow edge

    Attributes:
        status (str): edge status
        queue (tornado.queues.Queue): data queue
        fields (flashflood.lod.ListOfDict): edge status
        params (dict): optional parameters which will be sent to downstream
        sampler (flashflood.core.container.Sampler): data sampler object

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
        """Waits until all records are sent to its downstream node and then
        emits done signal.
        """
        yield self.queue.join()
        self.status = "done"

    @gen.coroutine
    def abort(self):
        """Waits until abort signal is propageted and all downstream tasks
        are completely stopped. Then emits abort signal.
        """
        yield self.queue.join()
        self.status = "aborted"
