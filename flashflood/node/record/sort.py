#
# (C) 2014-2018 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.sort import sort_key
from flashflood.core.node import IterNode


class NumericSort(IterNode):
    def __init__(self, order, **kwargs):
        super().__init__(**kwargs)
        self.order = order
        self._seen = set()
        self._mapping = {}

    def processor(self, rcds):
        rcds = list(rcds)
        for key, asc in reversed(self.order):
            rcds.sort(key=lambda x: sort_key(x[key]), reverse=not asc)
        for r in rcds:
            yield r
