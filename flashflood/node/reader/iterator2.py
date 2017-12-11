#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node2 import IterNode


class IteratorInput(IterNode):
    def __init__(self, records, fields=None, params=None):
        super().__init__(params=params)
        self.records = records
        if fields is not None:
            self.fields.merge(fields)

    def on_submitted(self):
        self._out_edge.records = self.records
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self.params)

    def run(self):
        self.on_start()
        self.on_finish()
