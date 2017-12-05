#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.containter import Container
from flashflood.node.function.apply import Apply, AsyncApply


class StdoutMonitor(Apply):
    def __init__(self, name, interval=10000, params=None):
        super().__init__(None, params=params)
        self.name = name
        self.interval = interval
        self.container = Container()
        self.func = self.counter

    def counter(self, row):
        self.container.value += 1
        if not self.container.value % self.interval:
            print("{}: {} rows processed".format(self.name,
                                                 self.container.value))
        return row


class AsyncStdoutMonitor(AsyncApply):
    def __init__(self, name, interval=10000, params=None):
        super().__init__(None, params=params)
        self.name = name
        self.interval = interval
        self.container = Container()
        self.func = self.counter

    def counter(self, row):
        self.container.value += 1
        if not self.container.value % self.interval:
            print("{}: {} rows processed".format(self.name,
                                                 self.container.value))
        return row
