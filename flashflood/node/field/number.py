#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from flashflood.core.node import IterNode, AsyncNode


class Number(IterNode):
    def __init__(self, key, counter=itertools.count, **kwargs):
        super().__init__(**kwargs)
        self.key = key
        self.counter = counter()
        if not self.fields:
            self.fields.add({"key": key, "name": key, "d3_format": "d"})

    def processor(self, rcds):
        for row, count in zip(rcds, self.counter):
            new_row = {self.key: count}
            new_row.update(row)
            yield new_row


class AsyncNumber(AsyncNode):
    def __init__(self, key, counter=itertools.count, **kwargs):
        super().__init__(**kwargs)
        self.key = key
        self.counter = counter()
        if not self.fields:
            self.fields.add({"key": key, "name": key, "d3_format": "d"})

    def process_record(self, rcd):
        new_row = {self.key: next(self.counter)}
        new_row.update(rcd)
        return new_row
