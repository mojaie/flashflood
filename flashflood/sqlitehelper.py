#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import os
import pickle

from chorus import molutil, smilessupplier, v2000reader
from chorus.draw import calc2dcoords
from chorus.model.graphmol import Compound

from flashflood import sqliteconnection as sqlite
from flashflood import configparser as conf
from flashflood.lod import LOD


SQLITE_RESOURCES = LOD(
    conf.schema()["resources"].filter("resourceType", "sqlite"))


# TODO: merge into sqliteconnection
class SQLiteHelper(object):
    def __init__(self):
        self.dbs = {}

    def origins_iter(self, rsrc_ids):
        for r in rsrc_ids:
            found = SQLITE_RESOURCES.find("id", r)
            self.dbs.setdefault(r, found["resourceFile"])
            conn = sqlite.Connection(
                os.path.join(conf.SQLITE_BASE_DIR, self.dbs[r]))
            yield r, found["table"], conn

    def record_count(self, rsrc_ids):
        count = 0
        for _, tbl, conn in self.origins_iter(rsrc_ids):
            count += conn.rows_count(tbl)
        return count

    def resource_fields(self, rsrc_ids):
        results = LOD()
        for r in rsrc_ids:
            found = SQLITE_RESOURCES.find("id", r)
            results.merge(found["fields"])
        return results

    def records_iter(self, rsrc_ids):
        for r, tbl, conn in self.origins_iter(rsrc_ids):
            for res in conn.rows_iter(tbl):
                row = dict(res)
                mol = Compound(pickle.loads(row["_molobj"]))
                row["_molobj"] = json.dumps(mol.jsonized())
                row["source"] = r
                yield row

    def search(self, rsrc_ids, key, value):
        key_exists = self.resource_fields(rsrc_ids).find("key", key)
        if key_exists is not None:
            for r, tbl, conn in self.origins_iter(rsrc_ids):
                res = conn.find_first(key, (value,), tbl)
                if res is not None:
                    row = dict(res)
                    if "_molobj" in row:
                        mol = Compound(pickle.loads(row["_molobj"]))
                        row["_molobj"] = json.dumps(mol.jsonized())
                        row["source"] = r
                    return row
        rsrcs = filter(lambda x: x["id"] in rsrc_ids, SQLITE_RESOURCES)
        if any(r["domain"] == "chemical" for r in rsrcs):
            null_record = {key: value}
            null_record["_molobj"] = json.dumps(
                molutil.null_molecule().jsonized())
            return null_record
        else:
            return {key: value}

    def find_all(self, rsrc_ids, key, values, op, fields=None):
        op = {"eq": "=", "gt": ">", "lt": "<", "ge": ">=", "le": "<=",
              "lk": "LIKE", "in": "IN"}[op]
        for r, tbl, conn in self.origins_iter(rsrc_ids):
            key_exists = self.resource_fields(rsrc_ids).find("key", key)
            if key_exists is None:
                continue
            for res in conn.find_iter(key, values, tbl, op):
                if fields is None:
                    row = dict(res)
                else:
                    row = {f: res[f] for f in fields if f in res.keys()}
                if "_molobj" in row:
                    mol = Compound(pickle.loads(row["_molobj"]))
                    row["_molobj"] = json.dumps(mol.jsonized())
                    row["source"] = r
                yield row

    def query_mol(self, query):
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
            res = self.search(
                (query["source"],), "compound_id", query["value"])
            if res is None:
                raise ValueError()
            qmol = Compound(json.loads(res["_molobj"]))
        return qmol


SQLITE_HELPER = SQLiteHelper()
