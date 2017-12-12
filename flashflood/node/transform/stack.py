#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import IterNode


class Stack(IterNode):
    def __init__(self, keys, skip_none=True, **kwargs):
        super().__init__(**kwargs)
        self.keys = keys
        self.skip_none = skip_none
        self.fields.merge([
            {"key": "field", "name": "field", "format": "text"},
            {"key": "value", "name": "value", "format": "text"}
        ])

    def merge_fields(self):
        fs = filter(lambda x: x["key"] in self.keys, self._in_edge.fields)
        self._out_edge.fields.merge(fs)
        self._out_edge.fields.merge(self.fields)

    def processor(self, rcds):
        for rcd in rcds:
            for nk in set(rcd.keys()) - set(self.keys):
                if rcd[nk] is None and self.skip_none:
                    continue
                d = {k: v for k, v in rcd.items() if k in self.keys}
                d["field"] = nk
                d["value"] = rcd[nk]
                yield d
