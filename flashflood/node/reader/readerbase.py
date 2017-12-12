#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.edge import IterEdge
from flashflood.core.node import Node
from flashflood.core.task import InvalidOperationError


class ReaderBase(Node):
    def __init__(self, sampler=None, **kwargs):
        super().__init__(**kwargs)
        self._out_edge = IterEdge(sampler)

    def add_in_edge(self, edge, port):
        raise InvalidOperationError("Input node cannot have upstream edges")

    def merge_fields(self):
        self._out_edge.fields.merge(self.fields)

    def update_params(self):
        self._out_edge.params.update(self.params)
