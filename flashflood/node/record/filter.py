#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood import sort
from flashflood.node.control.filter import Filter, AsyncFilter


class FilterRecords(Filter):
    def __init__(self, key, value, op="=", **kwargs):
        super().__init__(
            lambda x: sort.op_to_func[op](x[key], value),
            residue_counter=None, fields=None, **kwargs)


class AsyncFilterRecords(AsyncFilter):
    def __init__(self, key, value, op="=", **kwargs):
        super().__init__(
            lambda x: sort.op_to_func[op](x[key], value),
            residue_counter=None, fields=None, **kwargs)
