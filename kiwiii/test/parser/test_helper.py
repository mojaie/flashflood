#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import unittest

from kiwiii.parser import helper

TEST_FILE = os.path.join(
    os.path.dirname(__file__),
    "../../../resources/raw/instruments/SpectraMaxM2.txt"
)


class TestHelper(unittest.TestCase):
    def test_well_index(self):
        self.assertEqual(helper.well_index("A1"), 0)
        self.assertEqual(helper.well_index("A24"), 23)
        self.assertEqual(helper.well_index("P1"), 360)
        self.assertEqual(helper.well_index("P24"), 383)
        self.assertEqual(helper.well_index("A01"), 0)
        self.assertEqual(helper.well_index("p24"), 383)
