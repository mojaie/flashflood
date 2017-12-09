#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from tornado import gen

from flashflood import functional
from flashflood.core.node import AsyncNode, FunctionNode


def extend(key, source_key, func, in_place, fill, row):
    new_row = row.copy()
    try:
        new_row[key] = func(row[source_key])
    except KeyError:
        new_row[key] = fill
    if in_place and key != source_key:
        del new_row[source_key]
    return new_row


class Extend(FunctionNode):
    def __init__(self, key, source_key, func=functional.identity,
                 in_place=False, fill=None, fields=None, params=None):
        super().__init__(
            functools.partial(extend, key, source_key, func, in_place, fill),
            params=params)
        self.old_key = None
        if in_place and key != source_key:
            self.old_key = source_key
        if fields is not None:
            self.fields.merge(fields)

    def on_submitted(self):
        super().on_submitted()
        if self.old_key is not None:
            self._out_edge.fields.delete("key", self.old_key)


class AsyncExtend(AsyncNode):
    def __init__(self, key, source_key, func=functional.identity,
                 in_place=False, fill=None, fields=None, params=None):
        super().__init__(params=params)
        self.func = functools.partial(
            extend, key, source_key, func, in_place, fill)
        self.old_key = None
        if in_place and key != source_key:
            self.old_key = source_key
        if fields is not None:
            self.fields.merge(fields)

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            yield self._out_edge.put(self.func(in_))

    def on_submitted(self):
        super().on_submitted()
        if self.old_key is not None:
            self._out_edge.fields.delete("key", self.old_key)
