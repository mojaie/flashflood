#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood.core.node import Node
from flashflood.core.task import InvalidOperationError


class ContainerWriter(Node):
    def __init__(self, container, **kwargs):
        super().__init__(**kwargs)
        self.container = container

    def out_edge(self, port):
        raise InvalidOperationError("Output node cannot have downstream edges")

    def merge_fields(self):
        self.container.fields.merge(self._in_edge.fields)
        self.container.fields.merge(self.fields)

    def update_params(self):
        self.container.params.update(self._in_edge.params)
        self.container.params.update(self.params)

    @gen.coroutine
    def run(self, on_finish, on_abort):
        if self.edge_type(self._in_edge) == "AsyncEdge":
            self.synchronizer()
        while 1:
            if self._in_edge.status == "aborted":
                on_abort()
                break
            if self._in_edge.status == "done":
                if self.edge_type(self._in_edge) == "IterEdge":
                    self.container.records = list(self._in_edge.records)
                elif self.edge_type(self._in_edge) == "FuncEdge":
                    self.container.records = list(
                        map(self._in_edge.func, self._in_edge.records))
                on_finish()
                break
            yield gen.sleep(self.interval)

    @gen.coroutine
    def synchronizer(self):
        while 1:
            in_ = yield self._in_edge.get()
            self.container.records.append(in_)
