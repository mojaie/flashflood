#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import pickle

from tornado import gen

from flashflood import functional
from flashflood.core.edge import IterEdge, FuncEdge, AsyncEdge
from flashflood.core.task import TaskSpecs, InvalidOperationError
from flashflood.lod import ListOfDict


class Node(TaskSpecs):
    """Node base class

    Args:
        fields (flashflood.lod.ListOfDict): data fields
        params (dict): workflow variable dict

    Attributes:
        node_num (int): node ID number assigned by
            `flashflood.core.workflow.Workflow`
        fields (flashflood.lod.ListOfDict): data fields
        params (dict): workflow variable dict
        interval (float): worker thread loop interval time (in second)
    """
    def __init__(self, fields=None, params=None):
        self.node_num = None
        self.fields = ListOfDict(fields or [])
        self.params = dict(params or {})
        self.interval = 0.5
        self._in_edge = None
        self._out_edge = None

    @gen.coroutine
    def run(self, on_finish, on_abort):
        pass

    def on_submit(self):
        self.merge_fields()
        self.update_params()

    def on_start(self):
        pass

    def on_finish(self):
        pass

    def interrupt(self):
        pass

    def on_abort(self):
        pass

    def add_in_edge(self, edge, port):
        """Adds an incoming edge

        Args:
            edge(flashflood.core.edge.Edge): Edge object
            port(int): connection port
        """
        if port != 0:
            raise InvalidOperationError("invalid port")
        self._in_edge = edge

    def out_edge(self, port):
        """Returns an outgoing edge

        Args:
            port(int): connection port
        """
        if port != 0:
            raise InvalidOperationError("invalid port")
        return self._out_edge

    def edge_type(self, edge):
        """Returns edge type of the given Edge object"""
        return type(edge).__name__

    def merge_fields(self):
        """Pass fields to the downstream"""
        self._out_edge.fields.merge(self._in_edge.fields)
        self._out_edge.fields.merge(self.fields)

    def update_params(self):
        """Pass parameters to the downstream"""
        self._out_edge.params.update(self._in_edge.params)
        self._out_edge.params.update(self.params)


class IterNode(Node):
    """Iterator node (Synchronous worker) class

    IterNode has IterEdge as the outgoing edge and sends iterator object
    to the downstream. IterNode will synchronize incoming AsyncEdge and sends
    list of records to the downstream. IterNode send map object to the
    downstream if incoming FuncEdge has unapplied func and args.

    IterNode itself does not change contents of data records. IterNode should
    be inherited when you want to add some functionalities. Overriding
    IterNode.processor method may be a good practice to implement your own
    input generator.

    Args:
        sampler (flashflood.core.container.Sampler): record sampler
        **kwargs: kwargs
    """
    def __init__(self, sampler=None, **kwargs):
        super().__init__(**kwargs)
        self._out_edge = IterEdge(sampler)
        self._rcds_tmp = None

    @gen.coroutine
    def run(self, on_finish, on_abort):
        if self.edge_type(self._in_edge) == "AsyncEdge":
            self.synchronizer()
        while 1:
            if self._in_edge.status == "aborted":
                yield self._out_edge.abort()
                on_abort()
                break
            if self._in_edge.status == "done":
                if self.edge_type(self._in_edge) == "IterEdge":
                    self._out_edge.send(self.processor(self._in_edge.records))
                elif self.edge_type(self._in_edge) == "FuncEdge":
                    self._out_edge.send(self.processor(
                        map(self._in_edge.func, self._in_edge.records)))
                else:
                    self._out_edge.send(self.processor(self._rcds_tmp))
                on_finish()
                break
            yield gen.sleep(self.interval)

    @gen.coroutine
    def synchronizer(self):
        self._rcds_tmp = []
        while 1:
            in_ = yield self._in_edge.get()
            self._rcds_tmp.append(in_)

    def processor(self, rcds):
        for r in rcds:
            yield r


class FuncNode(Node):
    """Function and args node class.

    FuncNode sends function and arguments(records) to the downstrem. FuncNode
    will synchronize incoming AsyncEdge and sends list of records and its own
    function to the downstream. FuncNode.func should be picklable to be
    compatible with multiprocess computing (see ConcurrentNode).

    Args:
        func: function to be applied.
        sampler (flashflood.core.container.Sampler): record sampler
        **kwargs: kwargs
    """
    def __init__(self, func=functional.identity, sampler=None, **kwargs):
        super().__init__(**kwargs)
        self._out_edge = FuncEdge(sampler)
        self.func = func
        self._rcds_tmp = None
        try:
            pickle.dumps(self.func)
        except (AttributeError, TypeError):
            raise InvalidOperationError("FuncNode.func should be picklable")

    @gen.coroutine
    def run(self, on_finish, on_abort):
        if self.edge_type(self._in_edge) == "AsyncEdge":
            self.synchronizer()
        while 1:
            if self._in_edge.status == "aborted":
                yield self._out_edge.abort()
                on_abort()
                break
            if self._in_edge.status == "done":
                if self.edge_type(self._in_edge) == "IterEdge":
                    self._out_edge.send(self.func, self._in_edge.records)
                elif self.edge_type(self._in_edge) == "FuncEdge":
                    func = functional.compose(self.func, self._in_edge.func)
                    self._out_edge.send(func, self._in_edge.records)
                else:
                    self._out_edge.send(self.func, self._rcds_tmp)
                on_finish()
                break
            yield gen.sleep(self.interval)

    @gen.coroutine
    def synchronizer(self):
        self._rcds_tmp = []
        while 1:
            in_ = yield self._in_edge.get()
            self._rcds_tmp.append(in_)


class AsyncNode(Node):
    """Asynchronous worker node class.

    AsyncNode has a AsyncEdge implemented with asynchronous queue. If the
    incoming nodes have AsyncEdge, this will get records from the upstream
    queue and put records to the downstream immediately, without blocking.
    AsyncNode will asynchronize the incoming records if it is from sync edges
    (IterEdge and FuncEdge).

    AsyncNode itself does not change contents of data records. AsyncNode should
    be inherited when you want to add some functionalities. Overriding
    AsyncNode.process_record method may be a good practice to implement your
    own asynchronous worker nodes.

    Args:
        sampler (flashflood.core.container.Sampler): record sampler
        **kwargs: kwargs
    """
    def __init__(self, sampler=None, **kwargs):
        super().__init__(**kwargs)
        self._out_edge = AsyncEdge(sampler)
        self._interrupted = False

    @gen.coroutine
    def run(self, on_finish, on_abort):
        if self.edge_type(self._in_edge) == "AsyncEdge":
            self.async_loop()
        while 1:
            if self._in_edge.status == "aborted":
                yield self._out_edge.abort()
                on_abort()
                break
            if self._in_edge.status == "done":
                if self.edge_type(self._in_edge) == "AsyncEdge":
                    yield self._out_edge.done()
                    on_finish()
                else:
                    yield self.asynchronizer(on_finish, on_abort)
                break
            yield gen.sleep(self.interval)

    def interrupt(self):
        self._interrupted = True

    @gen.coroutine
    def async_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            yield self._out_edge.put(self.process_record(in_))

    @gen.coroutine
    def asynchronizer(self, on_finish, on_abort):
        if self.edge_type(self._in_edge) == "IterEdge":
            rcds = self._in_edge.records
        elif self.edge_type(self._in_edge) == "FuncEdge":
            rcds = map(self._in_edge.func, self._in_edge.records)
        for in_ in rcds:
            if self._interrupted:
                yield self._out_edge.abort()
                on_abort()
                return
            yield self._out_edge.put(self.process_record(in_))
        yield self._out_edge.done()
        on_finish()

    def process_record(self, rcd):
        return rcd
