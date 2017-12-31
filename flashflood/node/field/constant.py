#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood.core.node import FuncNode, AsyncNode


def constant(key, value, rcd):
    new_rcd = rcd.copy()
    new_rcd[key] = value
    return new_rcd


class ConstantField(FuncNode):
    def __init__(self, key, value, **kwargs):
        super().__init__(functools.partial(constant, key, value), **kwargs)
        self.old_key = None


class AsyncConstantField(AsyncNode):
    def __init__(self, key, value, **kwargs):
        super().__init__(**kwargs)
        self.func = functools.partial(constant, key, value)

    def process_record(self, rcd):
        return self.func(rcd)
