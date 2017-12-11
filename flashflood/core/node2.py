#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood import functional
from flashflood.core.task import Task
from flashflood.core.edge2 import IterEdge, FuncEdge, AsyncEdge


class Node(Task):
    """
    Parameters:
      status: str
        ready: ready to run
        done: finished and put all results to outgoing edges
    """
    def __init__(self, params=None):
        super().__init__()
        self.node_num = None
        self._in_edge = None
        self._out_edge = None
        self.out_edge_type = None
        self.interval = 0.5
        if params is not None:
            self.params.update(params)

    def add_in_edge(self, edge, port):
        if port != 0:
            raise ValueError("invalid port")
        self._in_edge = edge

    def out_edge(self, port):
        if port != 0:
            raise ValueError("invalid port")
        return self._out_edge

    def in_edge_type(self):
        return type(self._in_edge).__name__

    def out_edge_type(self):
        return type(self._out_edge).__name__

    def merge_fields(self):
        self._out_edge.fields.merge(self._in_edge.fields)
        self._out_edge.fields.merge(self.fields)

    def update_params(self):
        self._out_edge.params.update(self._in_edge.params)
        self._out_edge.params.update(self.params)


class IterNode(Node):
    def __init__(self, params=None, sampler=None):
        super().__init__(params=params)
        self._out_edge = IterEdge(sampler)

    def on_submitted(self):
        if self.in_edge_type() == "IterEdge":
            self.pass_records()
        elif self.in_edge_type() == "FuncEdge":
            self.apply_func()
        self.merge_fields()
        self.update_params()

    @gen.coroutine
    def run(self):
        self.on_start()
        if self.in_edge_type() != "AsyncEdge":
            self._out_edge.sample()
            self.on_finish()
            return
        self.synchronizer()
        while 1:
            if self._in_edge.status == "aborted":
                self._out_edge.sample()
                self._out_edge.status = "aborted"
                self.on_aborted()
                break
            if self._in_edge.status == "done":
                self._out_edge.sample()
                self._out_edge.status = "done"
                self.on_finish()
                break
            yield gen.sleep(self.interval)

    def pass_records(self):
        """IterEdge -> IterEdge"""
        self._out_edge.records = self._in_edge.records

    def apply_func(self):
        """FuncEdge -> IterEdge"""
        self._out_edge.records = map(self._in_edge.func, self._in_edge.records)

    @gen.coroutine
    def synchronizer(self):
        """AsyncEdge -> IterEdge, FuncEdge"""
        while 1:
            in_ = yield self._in_edge.get()
            self._out_edge.records.append(in_)


class FuncNode(Node):
    def __init__(self, func, params=None, sampler=None):
        super().__init__(params=params)
        self._out_edge = FuncEdge(sampler)
        self.func = func
        # TODO: pickle test

    def on_submitted(self):
        if self.in_edge_type() == "IterEdge":
            self.pass_func_args()
        elif self.in_edge_type() == "FuncEdge":
            self.compose()
        self.merge_fields()
        self.update_params()

    @gen.coroutine
    def run(self):
        self.on_start()
        if self.in_edge_type() != "AsyncEdge":
            self._out_edge.sample()
            self.on_finish()
            return
        self.synchronizer()
        while 1:
            if self._in_edge.status == "aborted":
                self._out_edge.sample()
                self._out_edge.status = "aborted"
                self.on_aborted()
                break
            if self._in_edge.status == "done":
                self._out_edge.sample()
                self._out_edge.status = "done"
                self.on_finish()
                break
            yield gen.sleep(self.interval)

    def pass_func_args(self):
        """IterEdge -> FuncEdge"""
        self._out_edge.func = self.func
        self._out_edge.records = self._in_edge.records

    def compose(self):
        """FuncEdge -> FuncEdge"""
        self._out_edge.func = functional.compose(self.func, self._in_edge.func)
        self._out_edge.records = self._in_edge.records

    @gen.coroutine
    def synchronizer(self):
        """AsyncEdge -> FuncEdge"""
        self._out_edge.func = self.func
        while 1:
            in_ = yield self._in_edge.get()
            self._out_edge.records.append(in_)


class AsyncNode(Node):
    def __init__(self, params=None, sampler=None):
        super().__init__(params=params)
        self._out_edge = AsyncEdge(sampler)

    def on_submitted(self):
        self.merge_fields()
        self.update_params()

    @gen.coroutine
    def run(self):
        self.on_start()
        if self.in_edge_type() != "AsyncEdge":
            yield self.asynchronizer()
            self.on_finish()
            return
        self.async_loop()
        while 1:
            if self._in_edge.status == "aborted":
                self._out_edge.status = "aborted"
                self.on_aborted()
                break
            if self._in_edge.status == "done":
                self._out_edge.status = "done"
                self.on_finish()
                break
            yield gen.sleep(self.interval)

    @gen.coroutine
    def interrupt(self):
        if self.status != "running":
            return
        self.status = "interrupted"
        yield self._out_edge.aborted()
        self.on_aborted()

    @gen.coroutine
    def async_loop(self):
        """AsyncEdge -> AsyncEdge"""
        while 1:
            in_ = yield self._in_edge.get()
            yield self._out_edge.put(in_)

    @gen.coroutine
    def asynchronizer(self):
        """IterEdge,FuncEdge -> AsyncEdge"""
        if self.in_edge_type() == "IterEdge":
            rcds = self._in_edge.records
        elif self.in_edge_type() == "FuncEdge":
            rcds = map(self._in_edge.func, self._in_edge.records)
        for in_ in rcds:
            if self.status == "interrupted":
                return
            yield self._out_edge.put(in_)
        yield self._out_edge.done()
