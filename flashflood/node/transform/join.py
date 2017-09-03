#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood.core.node import SyncNode


def leftjoin(key, mapping, row):
    new_row = row.copy()
    new_row.update(mapping.get(row[key], {}))
    return new_row


class LeftJoin(SyncNode):
    def __init__(self, key, right_key=None, params=None):
        super().__init__()
        self._left_in = None
        self._right_in = None
        self.key = key
        if right_key is None:
            self.right_key = key
        else:
            self.right_key = right_key
        if params is not None:
            self.params.update(params)

    def add_in_edge(self, edge, port):
        if port == 0:
            self._left_in = edge
        elif port == 1:
            self._right_in = edge
        else:
            raise ValueError("invalid port")

    def on_submitted(self):
        mapping = {r[self.right_key]: r for r in self._right_in.records}
        func = functools.partial(leftjoin, self.key, mapping)
        self._out_edge.records = map(func, self._left_in.records)
        self._out_edge.task_count = self._left_in.task_count
        self._out_edge.fields.merge(self._left_in.fields)
        self._out_edge.fields.merge(self._right_in.fields)
        self._out_edge.params.update(self._left_in.params)
        self._out_edge.params.update(self._right_in.params)
