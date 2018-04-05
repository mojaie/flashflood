#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import IterNode


class Unstack(IterNode):
    def __init__(self, keys, label, value, label_prefix="", **kwargs):
        super().__init__(**kwargs)
        self.keys = keys
        self.label = label
        self.value = value
        self.label_prefix = label_prefix
        self._mapping = {}

    def merge_fields(self):
        for f in self._in_edge.fields:
            if f["key"] in self.keys:
                self._out_edge.fields.add(f)
        # Note: label fields should be manually added
        self._out_edge.fields.merge(self.fields)

    def processor(self, rcds):
        kss = []
        for r in rcds:
            ks = tuple(r[k] for k in self.keys)
            new_key = "{}{}".format(self.label_prefix, r[self.label])
            kv = {new_key: r[self.value]}
            if ks in self._mapping:
                self._mapping[ks].update(kv)
                continue
            new_r = r.copy()
            new_r.update(kv)
            del new_r[self.value]
            self._mapping[ks] = new_r
            kss.append(ks)
        for ks in kss:
            yield self._mapping[ks]
