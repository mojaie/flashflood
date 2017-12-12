#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood.core.edge import IterEdge, FuncEdge, AsyncEdge
from flashflood.core.node import Node


class LeftJoin(Node):
    """Left join
        edge 0: left records edge
        edge 1: right records edge

        1. synchronize the right edge
        2. generate mapping from the right edge
        3. apply mapping to the left edge

    """
    def __init__(self, left_key, right_key, sampler=None, **kwargs):
        super().__init__(**kwargs)
        self._left_in = None
        self._right_in = None
        self.left_key = left_key
        self.right_key = right_key
        self._left_tmp = None
        self._right_tmp = None
        self.sampler = sampler

    def add_in_edge(self, edge, port):
        if port == 0:
            self._left_in = edge
        elif port == 1:
            self._right_in = edge
        else:
            raise ValueError("invalid port")

    def on_submit(self):
        if self.edge_type(self._left_in) == "IterEdge":
            self._out_edge = IterEdge(self.sampler)
        elif self.edge_type(self._left_in) == "FuncEdge":
            self._out_edge = FuncEdge(self.sampler)
        elif self.edge_type(self._left_in) == "AsyncEdge":
            self._out_edge = AsyncEdge(self.sampler)
        self.merge_fields()
        self.update_params()

    def merge_fields(self):
        self._out_edge.fields.merge(self._left_in.fields)
        self._out_edge.fields.merge(self._right_in.fields)
        self._out_edge.fields.merge(self.fields)

    def update_params(self):
        self._out_edge.params.update(self._left_in.params)
        self._out_edge.params.update(self._right_in.params)
        self._out_edge.params.update(self.params)

    @gen.coroutine
    def run(self, on_finish, on_abort):
        yield self.sync_right_in(on_abort)
        if self.edge_type(self._left_in) == "AsyncEdge":
            self.synchronizer()
        while 1:
            if self._left_in.status == "aborted":
                self._out_edge.status = "aborted"
                on_abort()
                break
            if self._left_in.status == "done":
                if self.edge_type(self._left_in) == "IterEdge":
                    self._out_edge.send(self.processor(self._left_in.records))
                elif self.edge_type(self._left_in) == "FuncEdge":
                    self._out_edge.send(self.processor(
                        map(self._left_in.func, self._left_in.records)))
                else:
                    self._out_edge.send(self.processor(self._left_tmp))
                on_finish()
                break
            yield gen.sleep(self.interval)

    @gen.coroutine
    def synchronizer(self):
        self._left_tmp = []
        while 1:
            in_ = yield self._left_in.get()
            self._left_tmp.append(in_)

    @gen.coroutine
    def sync_right_in(self, on_abort):
        """Synchronize right edge"""
        if self.edge_type(self._right_in) == "AsyncEdge":
            self.right_sync()
        while 1:
            if self._right_in.status == "aborted":
                on_abort()
                return
            if self._right_in.status == "done":
                if self.edge_type(self._right_in) == "IterEdge":
                    self._right_tmp = self._right_in.records
                elif self.edge_type(self._in_edge) == "FuncEdge":
                    self._right_tmp = map(
                        self._right_in.func, self._right_in.records)
                break
            yield gen.sleep(self.interval)

    @gen.coroutine
    def right_sync(self):
        self._right_tmp = []
        while 1:
            in_ = yield self._right_in.get()
            self._right_tmp.append(in_)

    def processor(self, rcds):
        mapping = {r[self.right_key]: r for r in self._right_tmp}
        for r in rcds:
            new_row = r.copy()
            new_row.update(mapping.get(r[self.left_key], {}))
            yield new_row
