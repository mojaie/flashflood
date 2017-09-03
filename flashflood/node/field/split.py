#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools

from flashflood.node.function.apply import Apply


def split(old_key, new_keys, separator, fill, row):
    new_row = row.copy()
    values = itertools.chain(
        row[old_key].split(separator), itertools.repeat(fill))
    for k in new_keys:
        new_row[k] = next(values)
    del new_row[old_key]
    return new_row


class SplitField(Apply):
    def __init__(self, old_key, new_keys, separator, fill=None,
                 fields=None, params=None):
        super().__init__(
            functools.partial(split, old_key, new_keys, separator, fill),
            fields=fields, params=params
        )
        self.old_key = old_key

    def on_submitted(self):
        super().on_submitted()
        self._out_edge.fields.delete("key", self.old_key)
