#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import unittest

from flashflood.parser import BiacoreT200 as bia

HERE = os.path.dirname(__file__)
TEST_FILE = os.path.join(HERE, "../../resources/raw/BiacoreT200.txt")


class TestBiacoreT200(unittest.TestCase):
    def test_file_loader(self):
        series = [
            {"conc": 50},
            {"conc": 100},
            {"conc": 200},
            {"conc": 400},
            {"conc": 800}
        ]
        rcds = bia.file_loader(TEST_FILE, series, 0, 5)
        self.assertEqual(len(rcds), 185)
