#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood.lod import ListOfDict
from flashflood.core.node import Node, Synchronizer
from flashflood.core.edge import Edge, FunctionEdge


class ContainerWriter(Node):
    def __init__(self, container, params=None):
        super().__init__(params=params)
        self.container = container

    def on_submitted(self):
        self.container.fields = ListOfDict()
        self.container.fields.merge(self._in_edge.fields)
        self.container.fields.merge(self.fields)
        self.container.params = {}
        self.container.params.update(self._in_edge.params)
        self.container.params.update(self.params)
        if isinstance(self._in_edge, Edge):
            rcds = self._in_edge.records
        elif isinstance(self._in_edge, FunctionEdge):
            rcds = map(self._in_edge.func, self._in_edge.records)
        else:
            raise ValueError("Invalid upstream edge.")
        self.container.records = list(rcds)


class AsyncContainerWriter(Synchronizer):
    def __init__(self, container, params=None):
        super().__init__(params=params)
        self.container = container
        self.container.records = []

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            self.container.records.append(in_)

    def on_submitted(self):
        self.container.fields = self._in_edge.fields
