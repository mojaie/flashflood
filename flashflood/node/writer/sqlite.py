#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import os
import sqlite3
import traceback

from flashflood.core.edge import FunctionEdge
from flashflood.core.node import Node
from flashflood.util import debug


# TODO: move to static
data_type = {
    "numeric": "real",
    "id": "text",
    "compound_id": "text",
    "assay_id": "text",
    "data_id": "text",
    "svg": "text",
    "json": "text",
    "plot": "text",
    "text": "text",
    "pickle": "blob",
    "pyobject": "blob",
    "image": "blob",
    "binary": "blob"
}


class SQLiteWriter(Node):
    def __init__(self, wf, dest_path, primary_key=None, create_index=None,
                 allow_overwrite=True, notice_per_records=10000):
        super().__init__()
        self._in_edges = []
        self.wf = wf
        self.dest_path = dest_path
        self.primary_key = primary_key
        self.create_index = create_index
        self.allow_overwrite = allow_overwrite
        self.notice_per_records = notice_per_records
        self.conn = None

    def add_in_edge(self, edge, port):
        if port != 0:
            raise ValueError("invalid port")
        self._in_edges.append(edge)

    @debug.profile
    def run(self):
        self.on_start()
        self.conn = sqlite3.connect(self.dest_path)
        self.conn.isolation_level = None
        cur = self.conn.cursor()
        cur.execute("PRAGMA page_size = 4096")
        cur.execute("BEGIN")
        try:
            # Truncate tables should be done in the transaction.
            if os.path.exists(self.dest_path) and self.allow_overwrite:
                print("Truncate existing database ...")
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cur.fetchall()]
                for t in tables:
                    cur.execute("DROP TABLE {}".format(t))
                    print("Table {} dropped".format(t))
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'")
                idxs = [row[0] for row in cur.fetchall()]
                for i in idxs:
                    cur.execute("DROP INDEX {}".format(i))
                    print("Index {} dropped".format(i))
            # Schema document table
            schema = self.wf.params
            schema["resources"] = []
            # Tables
            for in_edge in self._in_edges:
                # Create table
                phrases = []
                table_name = in_edge.params["table"]
                for field in in_edge.fields:
                    fieldphrase = field["key"]
                    if "d3_format" in field:
                        sqtype = "real"
                    else:
                        sqtype = data_type.get(field["format"], "blob")
                    fieldphrase += " {}".format(sqtype)
                    if self.primary_key is not None and \
                            field["key"] == self.primary_key:
                        fieldphrase += " primary key check({} != '')".format(
                            self.primary_key)
                    if sqtype == "text":
                        fieldphrase += " collate nocase"
                    phrases.append(fieldphrase)
                fielddef = ", ".join(phrases)
                sql = "CREATE TABLE {}({})".format(table_name, fielddef)
                cur.execute(sql)
                # Insert records
                if isinstance(in_edge, FunctionEdge):
                    records = map(in_edge.func, in_edge.records)
                else:
                    records = in_edge.records
                for i, rcd in enumerate(records):
                    fieldkeys = ", ".join(rcd.keys())
                    sql = "INSERT INTO {}({})".format(table_name, fieldkeys)
                    placeholders = ", ".join(["?"] * len(rcd))
                    sql += " VALUES({})".format(placeholders)
                    values = [rcd[c] for c in rcd.keys()]
                    try:
                        cur.execute(sql, values)
                    except sqlite3.IntegrityError as e:
                        print("skip #{}: {}".format(i, e))
                    if i and not i % self.notice_per_records:
                        print("{} rows processed...".format(i))
                cnt = cur.execute("SELECT COUNT(*) FROM {}".format(table_name))
                print("{} rows -> {}".format(cnt.fetchone()[0], table_name))
                # Create index
                if self.create_index is not None:
                    for k in self.create_index:
                        cur.execute("CREATE INDEX {}_{} ON {}({})".format(
                            table_name, k, table_name, k))
                        print("Create index {}_{} ...".format(table_name, k))
                # Append a resource schema
                table_schema = in_edge.params
                table_schema["fields"] = in_edge.fields
                schema["resources"].append(table_schema)
            # Save database schema
            cur.execute("CREATE TABLE document(document text)")
            cur.execute(
                "INSERT INTO document VALUES (?)", (json.dumps(schema),))
        except KeyboardInterrupt:
            print("User cancel")
            self.conn.rollback()
            self.conn.close()
            self.on_aborted()
        except:
            print(traceback.format_exc())
            self.conn.rollback()
            self.conn.close()
            self.on_aborted()
        else:
            self.conn.commit()
            print("Cleaning up...")
            cur.execute("VACUUM")
            self.conn.close()
            self.on_finish()

    def on_submitted(self):
        if os.path.exists(self.dest_path) and not self.allow_overwrite:
            raise ValueError("SQLite file already exists.")

    def interrupt(self):
        # TODO: core.workflow will call this when interrupted
        print("interrupt called")
        self.conn.interrupt()
