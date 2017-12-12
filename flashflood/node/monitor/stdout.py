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
        for i, r in enumerate(rcds):
            if not i % self.interval:
                print("{}: {} rows processed".format(self.name, i))
            yield r


class AsyncStdoutMonitor(AsyncNode):
    def __init__(self, name, interval=10000, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.interval = interval
        self.count = 0

    def process_record(self, rcd):
        if not self.count % self.interval:
            print("{}: {} rows processed".format(self.name, self.count))
        self.count += 1
        return rcd
