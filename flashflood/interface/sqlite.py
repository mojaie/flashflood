#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import sqlite3


class Connection(object):
    def __init__(self, path):
        con = sqlite3.connect(path)
        con.row_factory = sqlite3.Row
        self._cursor = con.cursor()

    def columns(self, table):
        """Returns list of columns"""
        return [
            row["name"] for row in self._cursor.execute(
                "pragma table_info('{}')".format(table)
            )
        ]

    def fetch_iter(self, query, values=(), arraysize=1000):
        """Execute custom fetch query"""
        self._cursor.execute(query, values)
        while True:
            # successive fetchmany(arraysize) instead of fetchAll()
            # improved performance and memory consumption
            rows = self._cursor.fetchmany(arraysize)
            if not rows:
                break
            for row in rows:
                yield dict(row)

    def fetch_one(self, query, values=()):
        """Execute custom fetch query"""
        self._cursor.execute(query, values)
        row = self._cursor.fetchone()
        if row is not None:
            return dict(row)

    def rows_iter(self, table, orderby=None, arraysize=1000):
        """Iterate over rows of tables"""
        if orderby is not None:
            orderby = " ORDER BY " + ", ".join(orderby)
        else:
            orderby = ""
        return self.fetch_iter(
            "SELECT * FROM {}{}".format(table, orderby), arraysize=arraysize
        )

    def rows_count(self, table):
        """Returns number of records of tables"""
        return self.fetch_one(
            "SELECT count(*) FROM {}".format(table)
        )["count(*)"]

    def find_all(self, table, key, value, op="=", arraysize=1000):
        """find records and return result generator"""
        return self.fetch_iter(
            "SELECT * FROM {} WHERE {} {} ?".format(table, key, op),
            values=(value,), arraysize=arraysize
        )

    def find_first(self, table, key, value):
        """find records and return first one

        Returns:
            dict: result rows
            None: if nothing is found
        """
        return self.fetch_one(
            "SELECT * FROM {} WHERE {} = ?".format(table, key), values=(value,)
        )
