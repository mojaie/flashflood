#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood.core.node import IterNode, AsyncNode


class Filter(IterNode):
    def __init__(self, pred, residue_counter=None, **kwargs):
        super().__init__(**kwargs)
        self.pred = pred
        self.residue_counter = residue_counter

    def processor(self, rcds):
        for r in rcds:
            if self.pred(r):
                yield r
            elif self.residue_counter is not None:
                self.residue_counter.value += 1


class AsyncFilter(AsyncNode):
    def __init__(self, pred, residue_counter=None, **kwargs):
        super().__init__(**kwargs)
        self.pred = pred
        self.residue_counter = residue_counter

    @gen.coroutine
    def async_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            if self.pred(in_):
                yield self._out_edge.put(in_)
            elif self.residue_counter is not None:
                self.residue_counter.value += 1

    @gen.coroutine
    def asynchronizer(self):
        if self.edge_type(self._in_edge) == "IterEdge":
            rcds = self._in_edge.records
        elif self.edge_type(self._in_edge) == "FuncEdge":
            rcds = map(self._in_edge.func, self._in_edge.records)
        for in_ in rcds:
            if self.status == "interrupted":
                return
            if self.pred(in_):
                yield self._out_edge.put(in_)
            elif self.residue_counter is not None:
                self.residue_counter.value += 1
        yield self._out_edge.done()
