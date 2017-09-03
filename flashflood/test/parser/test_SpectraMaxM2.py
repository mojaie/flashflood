#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import unittest

from flashflood.parser import SpectraMaxM2 as sm

TEST_FILE = os.path.join(
    os.path.dirname(__file__),
    "../../resources/raw/SpectraMaxM2.txt"
)


class TestSpextraMaxM2(unittest.TestCase):
    def test_file_loader(self):
        data = sm.file_loader(TEST_FILE)
        plate1 = data["plates"][0]
        self.assertEqual(plate1["plateId"], "Plate#1")
        self.assertEqual(plate1["layerIndex"], 0)
        self.assertEqual(plate1["wellValues"][0], "NaN")
        self.assertEqual(plate1["wellValues"][22], 11.125)
        self.assertEqual(plate1["wellValues"][361], 31.542)
        self.assertEqual(plate1["wellValues"][383], "NaN")
        plate2 = data["plates"][1]
        self.assertEqual(plate2["wellValues"][22], 11.918)
        self.assertEqual(plate2["wellValues"][382], 503.799)
