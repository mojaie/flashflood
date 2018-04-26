#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
from flashflood import sort
from flashflood.node.control.filter import Filter, AsyncFilter


def numfilter(key, value, op, rcd):
    try:
        float(rcd[key])
    except ValueError:
        return False
    return sort.op_to_func[op](float(rcd[key]), value)


class NumericFilter(Filter):
    def __init__(self, key, value, op="=", **kwargs):
        super().__init__(
            functools.partial(numfilter, key, value, op),
            residue_counter=None, fields=None, **kwargs)


class AsyncNumericFilter(AsyncFilter):
    def __init__(self, key, value, op="=", **kwargs):
        super().__init__(
            functools.partial(numfilter, key, value, op),
            residue_counter=None, fields=None, **kwargs)
