#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from flashflood.core.edge import Edge, FunctionEdge
from flashflood.core.node import SyncNode


class Combination(SyncNode):
    def __init__(self, r=2, params=None):
        super().__init__(params=params)
        self.r = r

    def on_submitted(self):
        super().on_submitted()
        if isinstance(self._in_edge, Edge):
            rcds = self._in_edge.records
        elif isinstance(self._in_edge, FunctionEdge):
            rcds = map(self._in_edge.func, self._in_edge.records)
        else:
            raise ValueError("Invalid upstream edge.")
        self._out_edge.records = itertools.combinations(rcds, self.r)
