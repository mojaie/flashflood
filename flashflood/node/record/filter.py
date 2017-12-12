#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import operator

from flashflood.node.control.filter import Filter, AsyncFilter


class FilterRecords(Filter):
    def __init__(self, key, value, filter_operator=operator.eq, **kwargs):
        super().__init__(
            lambda x: filter_operator(x[key], value),
            residue_counter=None, fields=None, **kwargs)


class AsyncFilterRecords(AsyncFilter):
    def __init__(self, key, value, filter_operator=operator.eq, **kwargs):
        super().__init__(
            lambda x: filter_operator(x[key], value),
            residue_counter=None, fields=None, **kwargs)
