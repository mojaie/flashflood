#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood.core.edge import IterEdge
from flashflood.core.node import Node
from flashflood.core.task import InvalidOperationError


class Replicate(Node):
    def __init__(self, n, **kwargs):
        super().__init__(**kwargs)
        self.n = n
        self._in_edge = None
        self._out_edges = [IterEdge() for _ in range(n)]
        self._rcds_tmp = None

    @gen.coroutine
    def run(self, on_finish, on_abort):
        if self.edge_type(self._in_edge) == "AsyncEdge":
            self.synchronizer()
        while 1:
            if self._in_edge.status == "aborted":
                for o in self._out_edges:
                    yield o.abort()
                on_abort()
                break
            if self._in_edge.status == "done":
                if self.edge_type(self._in_edge) == "IterEdge":
                    rcds = list(self._in_edge.records)
                    for o in self._out_edges:
                        o.send(self.processor(rcds[:]))
                elif self.edge_type(self._in_edge) == "FuncEdge":
                    rcds = list(self._in_edge.records)
                    for o in self._out_edges:
                        o.send(self.processor(
                            map(self._in_edge.func, rcds[:])
                        ))
                else:
                    for o in self._out_edges:
                        o.send(self.processor(self._rcds_tmp[:]))
                on_finish()
                break
            yield gen.sleep(self.interval)

    def out_edge(self, port):
        if port > self.n:
            raise InvalidOperationError("invalid port")
        return self._out_edges[port]

    def merge_fields(self):
        for o in self._out_edges:
            o.fields.merge(self._in_edge.fields)
            o.fields.merge(self.fields)

    def update_params(self):
        for o in self._out_edges:
            o.params.update(self._in_edge.params)
            o.params.update(self.params)

    @gen.coroutine
    def synchronizer(self):
        self._rcds_tmp = []
        while 1:
            in_ = yield self._in_edge.get()
            self._rcds_tmp.append(in_)

    def processor(self, rcds):
        for r in rcds:
            yield r
