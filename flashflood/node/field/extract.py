#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood.core.node import FuncNode


def extract(key, attrs, in_place, default, row):
    new_row = row.copy()
    for attr in attrs:
        try:
            v = row[key]
            nav = attr.split('.')
            for n in nav:
                v = v[n]
        except KeyError:
            v = default
        new_row[attr] = v
    if in_place and key not in attrs:
        del new_row[key]
    return new_row


# TODO: rename to Unnest
class Extract(FuncNode):
    def __init__(self, key, attrs, in_place=False, default=None, **kwargs):
        super().__init__(
            functools.partial(extract, key, attrs, in_place, default),
            **kwargs)
        self.old_key = None
        if in_place and key not in attrs:
            self.old_key = key

    def merge_fields(self):
        super().merge_fields()
        if self.old_key is not None:
            self._out_edge.fields.delete("key", self.old_key)
