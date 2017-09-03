#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood.node.function.apply import Apply


def extend(name, key, func, row):
    new_row = {name: func(key)}
    new_row.update(row)
    return new_row


class Extend(Apply):
    def __init__(self, name, create_from, apply_func=lambda x: x,
                 fields=None, params=None):
        super().__init__(
            functools.partial(extend, name, create_from, apply_func),
            fields=fields, params=params)
