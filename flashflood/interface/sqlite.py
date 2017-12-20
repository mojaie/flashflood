#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import pickle
import sqlite3

from chorus import smilessupplier, v2000reader
from chorus.draw import calc2dcoords
from chorus.model.graphmol import Compound

from flashflood import configparser as conf
from flashflood import lod


class Connection(object):
    def __init__(self, path):
        con = sqlite3.connect(path)
        con.row_factory = sqlite3.Row
        self._cursor = con.cursor()

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
        return dict(self._cursor.fetchone())

    def rows_iter(self, table, orderby=None, arraysize=1000):
        """Iterate over rows of tables"""
        if orderby is not None:
            orderby = " ORDER BY " + ", ".join(orderby)
        else:
            orderby = ""
        return self.fetch_iter(
            "SELECT * FROM {}{}".format(table, orderby), arraysize=arraysize)

    def rows_count(self, table):
        """Returns number of records of tables"""
        return self.fetch_one(
            "SELECT count(*) FROM {}".format(table))["count(*)"]

    def find_all(self, table, key, value, op="=", arraysize=1000):
        """find records and return result generator"""
        return self.fetch_iter(
            "SELECT * FROM {} WHERE {}{}?".format(table, key, op),
            values=(value,), arraysize=arraysize)

    def find_first(self, table, key, value):
        """find records and return first one

        Returns:
            dict: result rows
            None: if nothing is found
        """
        return self.fetch_one(
            "SELECT * FROM {} WHERE {}=?".format(table, key), values=(value,))


def find_resource(resource_id):
    rsrc = conf.RESOURCES.find("id", resource_id)
    path = os.path.join(conf.SQLITE_BASE_DIR, rsrc["resourceFile"])
    return path, rsrc["table"]


def resources(domain=None):
    rsrcs = lod.filtered(conf.RESOURCES, "resourceType", "sqlite")
    if domain is not None:
        rsrcs = lod.filtered(rsrcs, "domain", domain)
    for rsrc in rsrcs:
        path = os.path.join(conf.SQLITE_BASE_DIR, rsrc["resourceFile"])
        yield path, rsrc["table"]


def merged_fields(rsrc_ids):
    results = lod.ListOfDict()
    for r in rsrc_ids:
        rsrc = conf.RESOURCES.find("id", r)
        results.merge(rsrc["fields"])
    return results


def query_mol(query):
    if query["format"] == "smiles":
        try:
            qmol = smilessupplier.smiles_to_compound(query["value"])
            calc2dcoords.calc2dcoords(qmol)
        except (ValueError, StopIteration):
            raise TypeError()
    elif query["format"] == "molfile":
        try:
            qmol = v2000reader.mol_from_text(query["value"])
        except (ValueError, StopIteration):
            raise TypeError()
    elif query["format"] == "dbid":
        file_, table = find_resource(query["source"])
        conn = Connection(file_)
        res = conn.find_first(table, "compound_id", query["value"])
        if res is None:
            raise ValueError()
        qmol = Compound(pickle.loads(res["__molpickle"]))

    return qmol
