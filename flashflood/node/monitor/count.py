#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools
from tornado import gen

from flashflood.core.node import SyncNode, AsyncNode


class CountRows(SyncNode):
    def __init__(self, container, params=None):
        super().__init__(params=params)
        self.container = container

    def run(self):
        self.on_start()
        counter, main = itertools.tee(self._in_edge.records)
        self.container.value += sum(1 for _ in counter)
        self._out_edge.records = main
        self.on_finish()


class AsyncCountRows(AsyncNode):
    def __init__(self, container, params=None):
        super().__init__(params=params)
        self.container = container

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            yield self._out_edge.put(in_)
            self.container.value += 1

    @gen.coroutine
    def run(self):
        self.on_start()
        self._get_loop()
        while 1:
            if self._in_edge.status == "aborted":
                self._out_edge.status = "aborted"
                self.on_aborted()
                break
            if self._in_edge.status == "done":
                self._out_edge.status = "done"
                self.on_finish()
                break
            yield gen.sleep(self.interval)
