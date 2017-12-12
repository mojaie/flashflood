#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood.core.node import FuncNode


def concat(old_keys, new_key, separator, row):
    new_row = row.copy()
    new_row[new_key] = separator.join(row[k] for k in old_keys)
    for k in old_keys:
        del new_row[k]
    return new_row


class ConcatFields(FuncNode):
    def __init__(self, old_keys, new_key, separator="_", **kwargs):
        super().__init__(
           functools.partial(concat, old_keys, new_key, separator),
           **kwargs)
        self.old_keys = old_keys

    def merge_fields(self):
        super().merge_fields()
        for k in self.old_keys:
            self._out_edge.fields.delete("key", k)
