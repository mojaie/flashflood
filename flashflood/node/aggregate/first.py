#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import IterNode


class AggFirst(IterNode):
    def __init__(self, key, **kwargs):
        super().__init__(**kwargs)
        self.key = key
        self._seen = set()

    def processor(self, rcds):
        for r in rcds:
            k = r[self.key]
            if k in self._seen:
                continue
            self._seen.add(k)
            yield r
