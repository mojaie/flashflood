#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import IterNode, AsyncNode


class StdoutMonitor(IterNode):
    def __init__(self, name, interval=10000, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.interval = interval

    def processor(self, rcds):
        for i, rcd in enumerate(rcds):
            if not i % self.interval:
                print(rcd)
                print(f"{self.name}: {i} rows processed")
            yield rcd


class AsyncStdoutMonitor(AsyncNode):
    def __init__(self, name, interval=10000, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.interval = interval
        self.count = 0

    def process_record(self, rcd):
        if not self.count % self.interval:
            print(rcd)
            print(f"{self.name}: {self.count} rows processed")
        self.count += 1
        return rcd
