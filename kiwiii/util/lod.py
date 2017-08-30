#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#
"""List of dict (LOD) utility"""


def find(key, value, lod):
    """Returns the record found by the given key-value pair.
    if not found, return None
    """
    return next(filter(lambda x: x[key] == value, lod), None)


def filter_(key, value, lod):
    """Yields filtered records by the given key-value pair.
    """
    flt = filter(lambda x: x[key] == value, lod)
    while True:
        try:
            yield next(flt)
        except StopIteration:
            return


def join(key, left, right, full_join=False):
    """Join right LOD to the left LOD (in-place)"""
    idx = {}
    for L in left:
        idx[L[key]] = L
    for r in right:
        if r[key] in idx:
            idx[r[key]].update(r)
        elif full_join:
            left.append(r)
