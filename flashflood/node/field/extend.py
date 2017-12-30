#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood import functional
from flashflood.core.node import FuncNode, AsyncNode


def extend(key, source_key, func, in_place, fill, row):
    new_row = row.copy()
    try:
        new_row[key] = func(row[source_key])
    except KeyError:
        new_row[key] = fill
    else:
        if in_place and key != source_key:
            del new_row[source_key]
    return new_row


class Extend(FuncNode):
    def __init__(self, key, source_key, func=functional.identity,
                 in_place=False, fill=None, **kwargs):
        super().__init__(
            functools.partial(extend, key, source_key, func, in_place, fill),
            **kwargs)
        self.old_key = None
        if in_place and key != source_key:
            self.old_key = source_key

    def merge_fields(self):
        super().merge_fields()
        if self.old_key is not None:
            self._out_edge.fields.delete("key", self.old_key)


class AsyncExtend(AsyncNode):
    def __init__(self, key, source_key, func=functional.identity,
                 in_place=False, fill=None, **kwargs):
        super().__init__(**kwargs)
        self.func = functools.partial(
            extend, key, source_key, func, in_place, fill)
        self.old_key = None
        if in_place and key != source_key:
            self.old_key = source_key

    def merge_fields(self):
        super().merge_fields()
        if self.old_key is not None:
            self._out_edge.fields.delete("key", self.old_key)

    def process_record(self, rcd):
        return self.func(rcd)
