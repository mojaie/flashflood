#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools
from flashflood.core.node import IterNode


class Zip(IterNode):
    def __init__(self, keys, zip_keys, fill_none=True, **kwargs):
        super().__init__(**kwargs)
        self.keys = keys
        self.zip_keys = zip_keys
        self.fill_none = fill_none

    def processor(self, rcds):
        for rcd in rcds:
            filtered = []
            for zk in self.zip_keys:
                if rcd[zk] is None and self.fill_none:
                    filtered.append(itertools.repeat(None))
                else:
                    filtered.append(rcd[zk])
            for zipped in zip(*filtered):
                new_rcd = {k: v for k, v in zip(self.zip_keys, zipped)}
                for k in self.keys:
                    new_rcd[k] = rcd[k]
                yield new_rcd
