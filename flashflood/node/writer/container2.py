#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood.lod import ListOfDict
from flashflood.core.node2 import IterNode


class ContainerWriter(IterNode):
    def __init__(self, container, params=None):
        super().__init__(params=params)
        self.container = container

    def on_submitted(self):
        self.merge_fields()
        self.update_params()

    def merge_fields(self):
        self.container.fields = ListOfDict()
        self.container.fields.merge(self._in_edge.fields)
        self.container.fields.merge(self.fields)

    def update_params(self):
        self.container.params = {}
        self.container.params.update(self._in_edge.params)
        self.container.params.update(self.params)

    @gen.coroutine
    def run(self):
        self.on_start()
        if self.in_edge_type() == "IterEdge":
            self.container.records = list(self._in_edge.records)
            self.on_finish()
            return
        if self.in_edge_type() == "FuncEdge":
            self.container.records = list(
                map(self._in_edge.func, self._in_edge.records))
            self.on_finish()
            return
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
    def synchronizer(self):
        """AsyncEdge -> IterEdge, FuncEdge"""
        self.container.records = []
        while 1:
            in_ = yield self._in_edge.get()
            self.container.records.append(in_)
