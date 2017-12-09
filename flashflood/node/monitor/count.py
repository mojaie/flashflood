#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools
from tornado import gen

from flashflood.core.edge import Edge, FunctionEdge
from flashflood.core.node import SyncNode, AsyncNode


class CountRows(SyncNode):
    def __init__(self, container, params=None):
        super().__init__(params=params)
        self.container = container
        self.counter = None

    def on_submitted(self):
        super().on_submitted()
        if isinstance(self._in_edge, Edge):
            rcds = self._in_edge.records
        elif isinstance(self._in_edge, FunctionEdge):
            rcds = map(self._in_edge.func, self._in_edge.records)
        self.counter, self._out_edge.records = itertools.tee(rcds)

    def run(self):
        self.on_start()
        self.container.value += sum(1 for _ in self.counter)
        self.on_finish()


class AsyncCountRows(AsyncNode):
    def __init__(self, container, params=None):
        super().__init__(params=params)
        self.container = container

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            yield self._out_edge.put(in_)
            self.container.value += 1

    @gen.coroutine
    def run(self):
        self.on_start()
        self._get_loop()
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
