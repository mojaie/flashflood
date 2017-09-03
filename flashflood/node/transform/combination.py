#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from flashflood.core.node import SyncNode


class Combination(SyncNode):
    def __init__(self, r=2, params=None):
        super().__init__()
        self.r = r
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        comb = itertools.combinations(self._in_edge.records, self.r)
        main, counter = itertools.tee(comb)
        self._out_edge.records = main
        self._out_edge.task_count = sum(1 for i in counter)
        self._out_edge.fields.merge(self._in_edge.fields)
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self._in_edge.params)
        self._out_edge.params.update(self.params)
