#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools
from math import factorial as f

from flashflood.core.node import IterNode


class Combination(IterNode):
    def __init__(self, r=2, counter=None, **kwargs):
        super().__init__(**kwargs)
        self.r = r
        self.counter = counter

    def processor(self, rcds):
        if self.counter is not None:
            rcds = list(rcds)
            n = len(rcds)
            self.counter.value = f(n) // f(self.r) // f(n - self.r)
        return itertools.combinations(rcds, self.r)
