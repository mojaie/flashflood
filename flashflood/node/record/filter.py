#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import operator

from flashflood.node.function.filter import Filter


class FilterRecords(Filter):
    def __init__(self, key, value, filter_operator=operator.eq,
                 params=None):
        super().__init__(
            lambda x: filter_operator(x[key], value),
            params=params
        )
