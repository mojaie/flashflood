#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import IterNode


class Unstack(IterNode):
    def __init__(self, keys, label, value, options, **kwargs):
        super().__init__(**kwargs)
        self.keys = keys
        self.label = label
        self.value = value
        self.options = options
        self._mapping = {}

    def merge_fields(self):
        for f in self._in_edge.fields:
            if f["key"] in self.keys:
                self._out_edge.fields.add(f)
        ops = [
            {"key": o, "name": o, "format": "numeric"} for o in self.options
        ]
        self._out_edge.fields.merge(ops)
        self._out_edge.fields.merge(self.fields)

    def processor(self, rcds):
        kss = []
        for r in rcds:
            ks = tuple(r[k] for k in self.keys)
            kv = {r[self.label]: r[self.value]}
            if ks in self._mapping:
                self._mapping[ks].update(kv)
                continue
            self._mapping[ks] = {k: r[k] for k in self.keys}
            self._mapping[ks].update(kv)
            kss.append(ks)
        for ks in kss:
            yield self._mapping[ks]
