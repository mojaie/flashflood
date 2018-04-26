#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.node.control.filter import Filter, AsyncFilter


class Excludes(Filter):
    def __init__(self, key, seq, **kwargs):
        super().__init__(
            lambda x: x[key] not in seq,
            residue_counter=None, fields=None, **kwargs)


class AsyncExcludes(AsyncFilter):
    def __init__(self, key, seq, **kwargs):
        super().__init__(
            lambda x: x[key] not in seq,
            residue_counter=None, fields=None, **kwargs)
