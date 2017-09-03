#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import pickle
import unittest

from flashflood.lod import ListOfDict

records1 = [
    {"a": 12, "b": 24, "c": True, "d": "Alice"},
    {"a": 13, "b": 22, "c": False, "d": "Beth"},
    {"a": 14, "b": 28, "c": False, "d": "Charley"},
    {"a": 15, "b": 22, "c": False, "d": "Diana"}
]


class TestListOfDict(unittest.TestCase):
    def test_filter(self):
        data = ListOfDict(pickle.loads(pickle.dumps(records1)))
        self.assertEqual(len(list(data.filter("c", False))), 3)
        self.assertEqual(len(list(data.filter("d", "Tom"))), 0)
        # Alter a record parameter
        selected = list(data.filter("a", 15))
        selected[0]["b"] = 30
        self.assertEqual(data[3]["b"], 30)

    def test_values(self):
        data = ListOfDict(records1)
        self.assertEqual(sum(data.values("a")), 54)

    def test_reduce(self):
        data = ListOfDict(pickle.loads(pickle.dumps(records1)))
        data.unique("b")
        self.assertEqual(len(data), 3)

    def test_merge(self):
        data = ListOfDict(pickle.loads(pickle.dumps(records1)))
        records = [{"a": 12, "b": 16, "d": "Eliza"}]
        data.merge(records, "a", "update")
        self.assertEqual(len(data[0]), 4)
        self.assertEqual(data[0]["c"], True)
        data = ListOfDict(pickle.loads(pickle.dumps(records1)))
        data.merge(records, "a", "replace")
        self.assertEqual(len(data[0]), 3)
        data.merge(records, "a", "add")
        self.assertEqual(len(data), 5)

    def test_join(self):
        data = ListOfDict([
            {"a": 12, "b": 24},
            {"a": 13, "b": 26},
            {"a": 14, "b": 28}
        ])
        data2 = ListOfDict([
            {"a": 12, "c": True},
            {"a": 13, "c": False},
            {"a": 14, "c": False}
        ])
        data.join(data2, "a")
        self.assertEqual(data, [
            {"a": 12, "b": 24, "c": True},
            {"a": 13, "b": 26, "c": False},
            {"a": 14, "b": 28, "c": False}
        ])
        data3 = ListOfDict([
            {"a": 12, "d": "Alice"},
            {"a": 13, "d": "Beth"},
            {"a": 14, "d": "Charley"},
            {"a": 15, "d": "Diana"}
        ])
        data.join(data3, "a", True)
        self.assertEqual(data, [
            {"a": 12, "b": 24, "c": True, "d": "Alice"},
            {"a": 13, "b": 26, "c": False, "d": "Beth"},
            {"a": 14, "b": 28, "c": False, "d": "Charley"},
            {"a": 15, "d": "Diana"}
        ])

    def test_pick(self):
        data = ListOfDict(pickle.loads(pickle.dumps(records1)))
        rcd1 = data.pick("b", 22)
        self.assertEqual(rcd1, {"a": 13, "b": 22, "c": False, "d": "Beth"})
        rcd2 = data.pick("b", 22)
        self.assertEqual(rcd2, {"a": 15, "b": 22, "c": False, "d": "Diana"})
        self.assertEqual(len(data), 2)


if __name__ == '__main__':
    unittest.main()
