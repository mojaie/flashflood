#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from flashflood.core.node import SyncNode, AsyncNode


class Apply(SyncNode):
    def __init__(self, func, fields=None, params=None):
        super().__init__(params=params)
        self.func = func
        if fields is not None:
            self.fields.merge(fields)

    def on_submitted(self):
        super().on_submitted()
        self._out_edge.records = map(self.func, self._in_edge.records)


class AsyncApply(AsyncNode):
    def __init__(self, func, fields=None, params=None):
        super().__init__(params=params)
        self.func = func
        if fields is not None:
            self.fields.merge(fields)

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            out = self.func(in_)
            yield self._out_edge.put(out)
