#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import IterNode


class AggSum(IterNode):

    def __init__(self, key, value_key, sum_key="sum", **kwargs):
        super().__init__(**kwargs)
        self.key = key
        self.value_key = value_key
        self.sum_key = sum_key
        self._seen = set()
        self._mapping = {}

    def processor(self, rcds):
        for r in rcds:
            rcd = r.copy()
            k = rcd[self.key]
            if k in self._seen:
                self._mapping[k][self.sum_key] += rcd[self.value_key]
                continue
            self._seen.add(k)
            self._mapping[k] = rcd
            rcd[self.sum_key] = rcd[self.value_key]
            del rcd[self.value_key]
            yield rcd
