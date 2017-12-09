#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools


def identity(x):
    return x


def _compose3(f, g, x):
    return f(g(x))


def _compose2(f, g):
    return functools.partial(_compose3, f, g)


def compose(*funcs):
    """Function composition"""
    return functools.reduce(_compose2, funcs, identity)
