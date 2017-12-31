#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import pickle
import unittest

from flashflood.functional import compose


def f(x):
    return x ** 2


def g(x):
    return x * 2


def h(x):
    return x + 1


class TestFunctional(unittest.TestCase):
    def test_compose(self):
        composed = compose(f, g, h)
        # should be picklable
        pickle.dumps(composed)
        # f(g(h(x))
        self.assertEqual(composed(2), 36)


if __name__ == '__main__':
    unittest.main()
