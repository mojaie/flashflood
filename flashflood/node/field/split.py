#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools

from flashflood.core.node import FunctionNode


def split(old_key, new_keys, separator, fill, row):
    new_row = row.copy()
    values = itertools.chain(
        row[old_key].split(separator), itertools.repeat(fill))
    for k in new_keys:
        new_row[k] = next(values)
    del new_row[old_key]
    return new_row


class SplitField(FunctionNode):
    def __init__(self, old_key, new_keys, separator, fill=None,
                 fields=None, params=None):
        super().__init__(
            functools.partial(split, old_key, new_keys, separator, fill),
            params=params
        )
        self.old_key = old_key
        if fields is not None:
            self.fields.merge(fields)

    def on_submitted(self):
        super().on_submitted()
        self._out_edge.fields.delete("key", self.old_key)
