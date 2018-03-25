#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#
"""Graph utility"""

import pickle


def topological_sort(succs, preds):
    # Topological sort
    # TODO: is there an equivalent in NetworkX?
    order = []
    preds = pickle.loads(pickle.dumps(preds))
    succs = pickle.loads(pickle.dumps(succs))
    stack = [k for k, v in preds.items() if not v]
    while stack:
        n = stack.pop()
        order.append(n)
        for s in list(succs[n].keys()):
            del succs[n][s]
            del preds[s][n]
            if not len(preds[s].keys()):
                stack.append(s)
    if sum(len(s.keys()) for s in succs.values()):
        raise ValueError('This is not an acyclic graph.')
    return order
