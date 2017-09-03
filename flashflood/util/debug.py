#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#
"""Debug utility"""

import cProfile
import os
import pstats
import sys
from collections import deque
from itertools import chain


def total_size(obj, verbose=False):
    """Approximate memory size"""
    seen = set()

    def sizeof(o):
        if id(o) in seen:
            return 0
        seen.add(id(o))
        s = sys.getsizeof(o, default=0)
        if verbose:
            print(s, type(o), repr(o))
        if isinstance(o, (tuple, list, set, frozenset, deque)):
            s += sum(map(sizeof, iter(o)))
        elif isinstance(o, dict):
            s += sum(map(sizeof, chain.from_iterable(o.items())))
        elif "__dict__" in dir(o):
            s += sum(map(sizeof, chain.from_iterable(o.__dict__.items())))
        return s

    return sizeof(obj)


def total_size_str(obj):
    """Formatted approximate memory size"""
    s = total_size(obj)
    if 1 > s / 1000.0:
        return "{} Bytes".format(s)
    if 1 > s / 1000000.0:
        return "{} KB".format(round(s / 1000.0, 1))
    if 1 > s / 1000000000.0:
        return "{} MB".format(round(s / 1000000.0, 1))
    return "{} GB".format(round(s / 1000000000.0, 1))


def profile(func):
    """Decorator to execute cProfile"""
    def _f(*args, **kwargs):
        pr = cProfile.Profile()
        pr.enable()
        print("\n<<<---")
        res = func(*args, **kwargs)
        p = pstats.Stats(pr)
        p.strip_dirs().sort_stats('cumtime').print_stats(40)
        print("\n--->>>")
        return res

    return _f


def mute(func):
    """Decorator to mute stdout"""
    def _f(*args, **kwargs):
        sys.stdout = open(os.devnull, 'w')
        res = func(*args, **kwargs)
        sys.stdout.close()
        sys.stdout = sys.__stdout__
        return res

    return _f
