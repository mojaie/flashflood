#
# (C) 2014-2018 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import IterNode


class AggUniqList(IterNode):
    def __init__(self, key, value_key, list_key="list", **kwargs):
        super().__init__(**kwargs)
        self.key = key
        self.value_key = value_key
        self.list_key = list_key
        self._seen = set()
        self._mapping = {}

    def processor(self, rcds):
        for r in rcds:
            rcd = r.copy()
            k = rcd[self.key]
            v = rcd[self.value_key]
            if k in self._seen:
                self._mapping[k][self.list_key].add(v)
                continue
            self._seen.add(k)
            self._mapping[k] = rcd
            rcd[self.list_key] = {v}
            del rcd[self.value_key]
        for v in self._mapping.values():
            v[self.list_key] = list(v[self.list_key])
            yield v
