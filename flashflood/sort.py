#
# (C) 2014-2018 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools


def sort_cmp(a, b):
    """Numeric sort cmp function

    Args:
        a, b: values to compare

    Note:
        Text sort would be slower with this function
        so use sorted(values, key=str.lower) instead.
    """
    try:
        return a - b
    except TypeError:
        try:
            float(a)
        except (TypeError, ValueError):
            pass
        else:
            return -1
        try:
            float(b)
        except (TypeError, ValueError):
            pass
        else:
            return 1
        if str(a).lower() > str(b).lower():
            return 1
        elif str(a).lower() < str(b).lower():
            return -1
        else:
            return 0


sort_key = functools.cmp_to_key(sort_cmp)


def eq(a, b):
    return a == b


def lt(a, b):
    return sort_cmp(a, b) < 0


def le(a, b):
    return sort_cmp(a, b) <= 0


def gt(a, b):
    return sort_cmp(a, b) > 0


def ge(a, b):
    return sort_cmp(a, b) >= 0


op_to_func = {
    "=": eq,
    "eq": eq,
    "<": lt,
    "lt": lt,
    "<=": le,
    "le": le,
    ">": gt,
    "gt": gt,
    ">=": ge,
    "ge": ge
}
