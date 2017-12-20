#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood.core.node import FuncNode


def remove(key, row):
    new_row = row.copy()
    del new_row[key]
    return new_row


class RemoveField(FuncNode):
    def __init__(self, key, **kwargs):
        super().__init__(functools.partial(remove, key), **kwargs)
        self.key = key

    def merge_fields(self):
        super().merge_fields()
        self._out_edge.fields.delete("key", self.key)
