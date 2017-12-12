#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.node.reader.readerbase import ReaderBase


class IterInput(ReaderBase):
    def __init__(self, records, **kwargs):
        super().__init__(**kwargs)
        self.records = records

    def run(self, on_finish, on_abort):
        self._out_edge.send(self.records)
        on_finish()
