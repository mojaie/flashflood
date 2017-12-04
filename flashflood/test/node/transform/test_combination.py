#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from flashflood.node.reader.iterator import IteratorInput
from flashflood.node.transform.combination import Combination


def odd(dict_):
    if dict_["value"] % 2:
        return {"value": dict_["value"]}


class TestCombination(AsyncTestCase):
    @gen_test
    def test_combination(self):
        iter_in = IteratorInput({"value": i} for i in range(10))
        comb = Combination()
        comb.add_in_edge(iter_in.out_edge(0), 0)
        iter_in.on_submitted()
        comb.on_submitted()
        iter_in.run()
        yield comb.run()
        self.assertEqual(len(list(comb.out_edge(0).records)), 45)


if __name__ == '__main__':
    unittest.main()
