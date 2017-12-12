#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools

from flashflood.core.node import FuncNode


def split(old_key, new_keys, separator, fill, row):
    new_row = row.copy()
    values = itertools.chain(
        row[old_key].split(separator), itertools.repeat(fill))
    for k in new_keys:
        new_row[k] = next(values)
    del new_row[old_key]
    return new_row


class SplitField(FuncNode):
    def __init__(self, old_key, new_keys, separator, fill=None, **kwargs):
        super().__init__(
            functools.partial(split, old_key, new_keys, separator, fill),
            **kwargs
        )
        self.old_key = old_key

    def merge_fields(self):
        super().merge_fields()
        self._out_edge.fields.delete("key", self.old_key)
