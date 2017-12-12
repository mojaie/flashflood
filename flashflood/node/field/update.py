#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood.core.node import FuncNode


def rename(updates, row):
    new_row = {}
    for k, v in row.items():
        if k in updates:
            new_row[updates[k]] = v
        else:
            new_row[k] = v
    return new_row


class UpdateFields(FuncNode):
    def __init__(self, updates, **kwargs):
        super().__init__(functools.partial(rename, updates), **kwargs)
        self.updates = updates

    def merge_fields(self):
        super().merge_fields()
        for field in self._out_edge.fields:
            if field["key"] in self.updates:
                field["key"] = self.updates[field["key"]]
        self._out_edge.fields.reduce()
