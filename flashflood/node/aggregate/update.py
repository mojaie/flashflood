#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import SyncNode


class AggUpdate(SyncNode):
    def __init__(self, key, params=None):
        super().__init__()
        self.key = key
        self.seen = set()
        self.mapping = {}
        if params is not None:
            self.params.update(params)

    def update(self):
        for r in self._in_edge.records:
            k = r[self.key]
            if k in self.seen:
                self.mapping[k].update(r)
                continue
            self.seen.add(k)
            self.mapping[k] = r
            yield r

    def on_submitted(self):
        super().on_submitted()
        self._out_edge.records = self.update()
