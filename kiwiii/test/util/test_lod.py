#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from kiwiii.util import lod


class TestLOD(unittest.TestCase):
    def test_filter(self):
        data = [
            {"a": 12, "b": 24, "c": True, "d": "Alice"},
            {"a": 13, "b": 26, "c": False, "d": "Beth"},
            {"a": 14, "b": 28, "c": False, "d": "Charley"},
            {"a": 15, "b": 22, "c": False, "d": "Diana"}
        ]
        self.assertEqual(len(list(lod.filter_("c", False, data))), 3)
        self.assertEqual(len(list(lod.filter_("d", "Tom", data))), 0)
        # Alter a record parameter
        selected = list(lod.filter_("a", 15, data))
        selected[0]["b"] = 30
        self.assertEqual(data[3]["b"], 30)

    def test_join(self):
        data = [
            {"a": 12, "b": 24},
            {"a": 13, "b": 26},
            {"a": 14, "b": 28}
        ]
        data2 = [
            {"a": 12, "c": True},
            {"a": 13, "c": False},
            {"a": 14, "c": False}
        ]
        lod.join("a", data, data2)
        self.assertEqual(data, [
            {"a": 12, "b": 24, "c": True},
            {"a": 13, "b": 26, "c": False},
            {"a": 14, "b": 28, "c": False}
        ])
        data3 = [
            {"a": 12, "d": "Alice"},
            {"a": 13, "d": "Beth"},
            {"a": 14, "d": "Charley"},
            {"a": 15, "d": "Diana"}
        ]
        lod.join("a", data, data3, True)
        self.assertEqual(data, [
            {"a": 12, "b": 24, "c": True, "d": "Alice"},
            {"a": 13, "b": 26, "c": False, "d": "Beth"},
            {"a": 14, "b": 28, "c": False, "d": "Charley"},
            {"a": 15, "d": "Diana"}
        ])


if __name__ == '__main__':
    unittest.main()
