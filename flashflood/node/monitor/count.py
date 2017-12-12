#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import IterNode, AsyncNode


class CountRows(IterNode):
    def __init__(self, counter, **kwargs):
        super().__init__(**kwargs)
        self.counter = counter

    def processor(self, rcds):
        for r in rcds:
            self.counter.value += 1
            yield r


class AsyncCountRows(AsyncNode):
    def __init__(self, counter, **kwargs):
        super().__init__(**kwargs)
        self.counter = counter

    def process_record(self, rcd):
        self.counter.value += 1
        return rcd
