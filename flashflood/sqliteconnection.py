#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import sqlite3


class Connection(object):

    def __init__(self, path):
        con = sqlite3.connect(path)
        con.row_factory = sqlite3.Row
        self._cursor = con.cursor()

    def document(self):
        sql = "SELECT * FROM document"
        return json.loads(self._cursor.execute(sql).fetchone()["document"])

    def fetch_iter(self, query, arraysize=1000):
        """Execute custom fetch query"""
        self._cursor.execute(query)
        while True:
            # successive fetchmany(arraysize) instead of fetchAll()
            # improved performance and memory consumption
            rows = self._cursor.fetchmany(arraysize)
            if not rows:
                break
            for row in rows:
                yield row

    def rows_iter(self, table, orderby="", arraysize=1000):
        """Iterate over rows of tables"""
        if orderby != "":
            orderby = " ORDER BY " + ", ".join(orderby)
        sql = "SELECT * FROM {}{}".format(table, orderby)
        self._cursor.execute(sql)
        while True:
            # successive fetchmany(arraysize) instead of fetchAll()
            # improved performance and memory consumption
            rows = self._cursor.fetchmany(arraysize)
            if not rows:
                break
            for row in rows:
                yield row

    def rows_count(self, table):
        """Returns number of records of tables"""
        sql = "SELECT count(*) FROM {}".format(table)
        return self._cursor.execute(sql).fetchone()["count(*)"]

    def find_iter(self, key, values, table, op="=", arraysize=1000):
        """find records and return result generator"""
        if op == "IN":
            ph = "({})".format(", ".join(["?"] * len(values)))
            where = " WHERE {} in{}".format(key, ph)
        else:
            where = " WHERE {} {} ?".format(key, op)
        sql = "SELECT * FROM {}{}".format(table, where)
        self._cursor.execute(sql, values)
        while True:
            rows = self._cursor.fetchmany(arraysize)
            if not rows:
                break
            for row in rows:
                yield row

    def find_first(self, key, values, table):
        """find records and return first one

        Returns:
            dict: result rows
            None: if nothing is found
        """
        sql = "SELECT * FROM {} WHERE {}=?".format(table, key)
        res = self._cursor.execute(sql, values).fetchone()
        if res:
            return res
