#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from kiwiii.sqliteconnection import Connection


@unittest.skip("TODO: takes long time")
class TestSQLiteConnector(unittest.TestCase):
    def setUp(self):
        self.conn = Connection("./datasource/sdf_demo.sqlite3")

    def test_schema(self):
        doc = self.conn.document()
        self.assertEqual(len(doc["tables"]), 2)
        self.assertEqual(doc["tables"][0]["id"], "drugbankfda")
        self.assertEqual(len(doc["tables"][0]["columns"]), 3)

    def test_rows_iter(self):
        res = self.conn.rows_iter(("DRUGBANKFDA",))
        self.assertEqual(len(list(res)), 1543)

    def test_rows_count(self):
        cnt = self.conn.rows_count(("DRUGBANKFDA",))
        self.assertEqual(cnt, 1543)
        cnt = self.conn.rows_count(("DRUGBANKALL",))
        self.assertEqual(cnt, 7049)
        cnt = self.conn.rows_count(("DRUGBANKFDA", "DRUGBANKALL"))
        self.assertEqual(cnt, 8592)
        cnt = self.conn.rows_count()
        self.assertEqual(cnt, 8592)

    def test_find_first(self):
        res = self.conn.find_first(
            "ID", ("DB00928",), ("DRUGBANKFDA",)
        )
        self.assertEqual(res["NAME"], "Azacitidine")
        res = self.conn.find_first(
            "ID", ("DB00000",), ("DRUGBANKFDA",)
        )
        self.assertEqual(res, None)

    def test_find_iter(self):
        res = self.conn.find_iter(
            "ID", ("DB00928",), ("DRUGBANKFDA",), "="
        )
        self.assertEqual(next(res)["NAME"], "Azacitidine")


if __name__ == '__main__':
    unittest.main()
