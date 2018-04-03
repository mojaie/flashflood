#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from flashflood.sort import sort_cmp, sort_key


class TestSort(unittest.TestCase):
    def test_sort(self):
        self.assertLess(sort_cmp(1, 2), 0)
        self.assertGreater(sort_cmp(1, 4.3e-5), 0)
        self.assertLess(sort_cmp(1, '-'), 0)  # ASCII 0020-002F
        self.assertGreater(sort_cmp('-', 1), 0)  # ASCII 0020-002F
        self.assertLess(sort_cmp(1, 'hoge'), 0)
        self.assertLess(sort_cmp('fuga', 'hoge'), 0)
        self.assertLess(sort_cmp('fuga', None), 0)
        self.assertLess(sort_cmp(int, str), 0)
        self.assertLess(sort_cmp([1, 2, 3], {1, 2, 3}), 0)
        self.assertTrue(sort_key(1) < sort_key(2))
        self.assertTrue(sort_key('fuga') < sort_key('hoge'))


if __name__ == '__main__':
    unittest.main()
