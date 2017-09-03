#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from flashflood.core.node import SyncNode


def stack(row, keys, skip_none):
    for nk in set(row.keys()) - set(keys):
        if row[nk] is None and skip_none:
            continue
        d = {k: v for k, v in row.items() if k in keys}
        d["_field"] = nk
        d["_value"] = row[nk]
        yield d


class Stack(SyncNode):
    def __init__(self, keys, skip_none=True, params=None):
        super().__init__()
        self.keys = keys
        self.skip_none = skip_none
        if params is not None:
            self.params.update(params)
        self.fields = [
            {"key": "_field", "name": "field", "valueType": "text"},
            {"key": "_value", "name": "value", "valueType": "text"}
        ]

    def on_submitted(self):
        stacked = itertools.chain.from_iterable(
            stack(rcd, self.keys, self.skip_none)
            for rcd in self._in_edge.records)
        main, counter = itertools.tee(stacked)
        self._out_edge.records = main
        self._out_edge.task_count = sum(1 for i in counter)
        fs = filter(lambda x: x["key"] in self.keys, self._in_edge.fields)
        self._out_edge.fields.merge(fs)
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self._in_edge.params)
        self._out_edge.params.update(self.params)
