#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools

from tornado import gen

from flashflood import static
from flashflood.core.node import SyncNode, AsyncNode


def number(name, zipped):
    row, count = zipped
    new_row = {name: count}
    new_row.update(row)
    return new_row


class Number(SyncNode):
    def __init__(self, name=None, counter=itertools.count,
                 fields=None, params=None):
        super().__init__()
        self.counter = counter
        if name is None:
            name = "_index"
        if fields is None:
            fields = [static.INDEX_FIELD]
        self.fields.merge(fields)
        self.func = functools.partial(number, name)
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        super().on_submitted()
        zipped = zip(self._in_edge.records, self.counter())
        self._out_edge.records = map(self.func, zipped)


class AsyncNumber(AsyncNode):
    def __init__(self, name="_index", counter=itertools.count,
                 fields=None, params=None):
        super().__init__()
        self.counter = counter
        if name is None:
            name = "_index"
        if fields is None:
            fields = [static.INDEX_FIELD]
        self.fields.merge(fields)
        self.func = functools.partial(number, name)
        if params is not None:
            self.params.update(params)

    @gen.coroutine
    def _get_loop(self):
        cnt = self.counter()
        while 1:
            in_ = yield self._in_edge.get()
            yield self._out_edge.put(self.func((in_, next(cnt))))
            self._out_edge.done_count = self._in_edge.done_count
