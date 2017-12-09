#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood.core.node import FunctionNode


def concat(old_keys, new_key, separator, row):
    new_row = row.copy()
    new_row[new_key] = separator.join(row[k] for k in old_keys)
    for k in old_keys:
        del new_row[k]
    return new_row


class ConcatFields(FunctionNode):
    def __init__(self, old_keys, new_key, separator="_",
                 fields=None, params=None):
        super().__init__(
           functools.partial(concat, old_keys, new_key, separator),
           params=params)
        self.old_keys = old_keys
        if fields is not None:
            self.fields.merge(fields)

    def on_submitted(self):
        super().on_submitted()
        for k in self.old_keys:
            self._out_edge.fields.delete("key", k)
