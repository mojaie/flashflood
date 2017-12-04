#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood.core.node import Node, Synchronizer


class ContainerWriter(Node):
    def __init__(self, container, params=None):
        super().__init__(params)
        self.container = container

    def run(self):
        self.on_start()
        self.container.records = list(self._in_edge.records)
        self.on_finish()

    def on_submitted(self):
        self.container.fields = self._in_edge.fields


class AsyncContainerWriter(Synchronizer):
    def __init__(self, container, params=None):
        super().__init__(params)
        self.container = container
        self.container.records = []

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            self.container.records.append(in_)

    def on_submitted(self):
        self.container.fields = self._in_edge.fields
