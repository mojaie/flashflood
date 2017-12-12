#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import IterNode


class AggUpdate(IterNode):
    def __init__(self, key, **kwargs):
        super().__init__(**kwargs)
        self.key = key
        self._seen = set()
        self._mapping = {}

    def processor(self, rcds):
        for r in rcds:
            k = r[self.key]
            if k in self._seen:
                self._mapping[k].update(r)
                continue
            self._seen.add(k)
            self._mapping[k] = r
            yield r
