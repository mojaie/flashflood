#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import SyncNode


class AggFirst(SyncNode):
    def __init__(self, key, params=None):
        super().__init__()
        self.key = key
        self.seen = set()
        if params is not None:
            self.params.update(params)

    def first(self):
        for r in self._in_edge.records:
            k = r[self.key]
            if k in self.seen:
                continue
            self.seen.add(k)
            yield r

    def on_submitted(self):
        super().on_submitted()
        self._out_edge.records = self.first()
