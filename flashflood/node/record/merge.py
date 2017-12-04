#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from flashflood.core.node import SyncNode


class MergeRecords(SyncNode):
    def __init__(self, params=None):
        super().__init__(params=params)
        self._in_edges = []

    def add_in_edge(self, edge, port):
        if port != 0:
            raise ValueError("invalid port")
        self._in_edges.append(edge)

    def on_submitted(self):
        self._out_edge.records = itertools.chain.from_iterable(
            i.records for i in self._in_edges)
        for e in self._in_edges:
            self._out_edge.fields.merge(e.fields)
        for e in self._in_edges:
            self._out_edge.params.update(e.params)
        self._out_edge.params.update(self.params)
