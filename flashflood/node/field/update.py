#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood.core.node import FunctionNode


def rename(updates, row):
    new_row = {}
    for k, v in row.items():
        if k in updates:
            new_row[updates[k]] = v
        else:
            new_row[k] = v
    return new_row


class UpdateFields(FunctionNode):
    def __init__(self, updates, fields=None, params=None):
        super().__init__(functools.partial(rename, updates), params=params)
        self.updates = updates
        if fields is not None:
            self.fields.merge(fields)

    def on_submitted(self):
        super().on_submitted()
        for field in self._out_edge.fields:
            if field["key"] in self.updates:
                field["key"] = self.updates[field["key"]]
        self._out_edge.fields.reduce()
