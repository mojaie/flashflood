#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from flashflood.util import graph


def succs_to_preds(succs):
    preds = {s: {} for s in succs.keys()}
    for u, s in succs.items():
        for v in s.keys():
            preds[v][u] = {}
    return preds


class TestGraph(unittest.TestCase):
    def test_topological_sort(self):
        succs = {
            0: {1: {}},
            1: {2: {}},
            2: {3: {}},
            3: {}
        }
        preds = succs_to_preds(succs)
        self.assertEqual(graph.topological_sort(succs, preds), [0, 1, 2, 3])
        succs = {
            0: {1: {}},
            1: {2: {}},
            2: {5: {}},
            3: {4: {}},
            4: {5: {}},
            5: {},
            6: {}
        }
        preds = succs_to_preds(succs)
        self.assertEqual(graph.topological_sort(succs, preds)[-1], 5)
        # cycle
        succs = {
            0: {1: {}},
            1: {2: {}},
            2: {3: {}},
            3: {0: {}},
        }
        preds = succs_to_preds(succs)
        with self.assertRaises(ValueError):
            graph.topological_sort(succs, preds)


if __name__ == '__main__':
    unittest.main()
