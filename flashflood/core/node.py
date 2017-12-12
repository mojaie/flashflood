#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood import functional
from flashflood.core.edge import IterEdge, FuncEdge, AsyncEdge
from flashflood.core.task import TaskSpecs, InvalidOperationError
from flashflood.lod import ListOfDict


class Node(TaskSpecs):
    """Node base class

    Args:
        fields: data fields
        params: workflow variable dict

    Parameters:
        node_num: node number assigned by `Workflow`
        fields: data fields
        params: workflow variable dict
        interval: worker thread loop interval
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
        """Delegated by Task.return

        Args:
            on_finish(callable): Task.on_finish callback
            on_abort(callable): Task.on_abort callback
        """
        pass

    def on_submit(self):
        """Delegated by Task.on_submit"""
        self.merge_fields()
        self.update_params()

    def on_start(self):
        """Delegated by Task.on_start"""
        pass

    def on_finish(self):
        """Delegated by Task.on_finish"""
        pass

    def interrupt(self):
        """Delegated by Task.interrupt"""
        pass

    def on_abort(self):
        """Delegated by Task.on_abort"""
        pass

    def add_in_edge(self, edge, port):
        """Adds an incoming edge

        Args:
            edge(core.edge.Edge): Edge object
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

    Args:
        sampler(container.Sampler): record sampler
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
    The node is intended for usage in multiprocess computing
    so it should be picklable.

    Args:
        func: function to be applied.
        sampler(container.Sampler): record sampler
        **kwargs: kwargs
    """
    def __init__(self, func=functional.identity, sampler=None, **kwargs):
        super().__init__(**kwargs)
        self._out_edge = FuncEdge(sampler)
        self.func = func
        self._rcds_tmp = None
        # TODO: pickle test

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

    Args:
        sampler(container.Sampler): record sampler
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
