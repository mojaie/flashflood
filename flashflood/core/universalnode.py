#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood import functional
from flashflood.core.task import Task


class UniversalNode(Task):
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

    def on_submitted(self):
        if self.in_edge_type() == "Edge":
            if self.out_edge_type() == "Edge":
                self.pass_records()
            elif self.out_edge_type() == "FunctionEdge":
                self.pass_func_args()
        elif self.in_edge_type() == "FunctionEdge":
            if self.out_edge_type() == "Edge":
                self.apply_func()
            elif self.out_edge_type() == "FunctionEdge":
                self.compose()
        self.merge_fields()
        self.update_params()

    @gen.coroutine
    def run(self):
        self.on_start()
        if self.in_edge_type() != "AsyncQueueEdge":
            # SyncInEdge
            if self.out_edge_type() == "AsyncQueueEdge":
                yield self.asynchronizer()
            self.on_finish()
            return
        # AsyncInEdge
        if self.out_edge_type() == "AsyncQueueEdge":
            self.async_loop()
        else:
            self.synchronizer()
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

    def pass_records(self):
        """Edge -> Edge"""
        self._out_edge.records = self._in_edge.records

    def pass_func_args(self):
        """Edge -> FunctionEdge"""
        self._out_edge.func = self.func
        self._out_edge.records = self._in_edge.records

    def apply_func(self):
        """FunctionEdge -> Edge"""
        self._out_edge.records = map(self._in_edge.func, self._in_edge.records)

    def compose(self):
        """FunctionEdge -> FunctionEdge"""
        self._out_edge.func = functional.compose(self.func, self._in_edge.func)
        self._out_edge.records = self._in_edge.records

    @gen.coroutine
    def async_loop(self):
        """AsyncQueueEdge -> AsyncQueueEdge"""
        while 1:
            in_ = yield self._in_edge.get()
            yield self._out_edge.put(in_)

    @gen.coroutine
    def synchronizer(self):
        """AsyncQueueEdge -> Edge,FunctionEdge"""
        while 1:
            in_ = yield self._in_edge.get()
            self._out_edge.records.append(in_)

    @gen.coroutine
    def asynchronizer(self):
        """Edge,FunctionEdge -> AsyncQueueEdge"""
        for in_ in self._in_edge.records:
            if self.status == "interrupted":
                return
            yield self._out_edge.put(in_)
        yield self._out_edge.done()

    def add_in_edge(self, edge, port):
        if port != 0:
            raise ValueError("invalid port")
        self._in_edge = edge
        self.in_edge_type = type(self._in_edge)

    def out_edge(self, port):
        if port != 0:
            raise ValueError("invalid port")
        return self._out_edge

    def merge_fields(self):
        self._out_edge.fields.merge(self._in_edge.fields)
        self._out_edge.fields.merge(self.fields)

    def update_params(self):
        self._out_edge.params.update(self._in_edge.params)
        self._out_edge.params.update(self.params)
