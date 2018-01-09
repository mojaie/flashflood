#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from tornado import gen
from flashflood.core.edge import IterEdge, AsyncEdge
from flashflood.core.node import Node
from flashflood.core.task import InvalidOperationError


class MergeRecords(Node):
    def __init__(self, sampler=None, **kwargs):
        super().__init__(**kwargs)
        self._in_edges = []
        self._rcds_tmp = None
        self._out_edge = IterEdge(sampler)

    def add_in_edge(self, edge, port):
        if port != 0:
            raise InvalidOperationError("invalid port")
        self._in_edges.append(edge)

    @gen.coroutine
    def run(self, on_finish, on_abort):
        rcds = []
        for edge in self._in_edges:
            if self.edge_type(edge) == "AsyncEdge":
                self.synchronizer(edge)
            while 1:
                if edge.status == "aborted":
                    on_abort()
                    return
                if edge.status == "done":
                    if self.edge_type(edge) == "IterEdge":
                        rcds.append(edge.records)
                    elif self.edge_type(edge) == "FuncEdge":
                        rcds.append(map(edge.func, edge.records))
                    elif self.edge_type(edge) == "AsyncEdge":
                        rcds.append(self._rcds_tmp)
                    break
                yield gen.sleep(self.interval)
        self._out_edge.send(itertools.chain.from_iterable(rcds))
        on_finish()

    @gen.coroutine
    def synchronizer(self, edge):
        self._rcds_tmp = []
        while 1:
            in_ = yield edge.get()
            self._rcds_tmp.append(in_)

    def merge_fields(self):
        for e in self._in_edges:
            self._out_edge.fields.merge(e.fields)
        self._out_edge.fields.merge(self.fields)

    def update_params(self):
        for e in self._in_edges:
            self._out_edge.params.update(e.params)
        self._out_edge.params.update(self.params)


class AsyncMergeRecords(Node):
    def __init__(self, sampler=None, **kwargs):
        super().__init__(**kwargs)
        self._in_edges = []
        self._rcds_tmp = None
        self._out_edge = AsyncEdge(sampler)
        self._interrupted = False

    def add_in_edge(self, edge, port):
        if port != 0:
            raise InvalidOperationError("invalid port")
        self._in_edges.append(edge)

    @gen.coroutine
    def run(self, on_finish, on_abort):
        for edge in self._in_edges:
            if self.edge_type(edge) == "AsyncEdge":
                self.async_loop(edge)
            while 1:
                if edge.status == "aborted":
                    yield self._out_edge.abort()
                    on_abort()
                    return
                if edge.status == "done":
                    if self.edge_type(edge) == "AsyncEdge":
                        yield self._out_edge.done()
                        on_finish()
                    else:
                        yield self.asynchronizer(edge, on_finish, on_abort)
                    break
                yield gen.sleep(self.interval)

    def interrupt(self):
        self._interrupted = True

    @gen.coroutine
    def async_loop(self, edge):
        while 1:
            in_ = yield edge.get()
            yield self._out_edge.put(self.process_record(in_))

    @gen.coroutine
    def asynchronizer(self, edge, on_finish, on_abort):
        if self.edge_type(edge) == "IterEdge":
            rcds = edge.records
        elif self.edge_type(edge) == "FuncEdge":
            rcds = map(edge.func, edge.records)
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

    def merge_fields(self):
        for e in self._in_edges:
            self._out_edge.fields.merge(e.fields)
        self._out_edge.fields.merge(self.fields)

    def update_params(self):
        for e in self._in_edges:
            self._out_edge.params.update(e.params)
        self._out_edge.params.update(self.params)
