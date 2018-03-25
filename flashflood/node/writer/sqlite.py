#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import yaml
import os
import sqlite3
import traceback
import warnings

from tornado import gen

from flashflood import static
from flashflood.core.node import Node
from flashflood.core.task import (
    InvalidOperationError, UnexpectedOperationWarning)
from flashflood.lod import ListOfDict
from flashflood import debug


class SQLiteWriter(Node):
    def __init__(self, dest_path, primary_key=None, create_index=None,
                 schema_file=True, allow_overwrite=True,
                 notice_per_records=10000, **kwargs):
        super().__init__(**kwargs)
        self._in_edges = []
        self.dest_path = dest_path
        self.primary_key = primary_key
        self.create_index = create_index
        self.schema_file = schema_file
        self.allow_overwrite = allow_overwrite
        self.notice_per_records = notice_per_records
        self.conn = None
        self._rcds_tmp = None
        self._results = None
        if self.fields:
            warnings.warn(
                "SQLiteWriter: Custom fields will be ignored",
                UnexpectedOperationWarning)
        if self.params:
            warnings.warn(
                "SQLiteWriter: Custom params will be ignored",
                UnexpectedOperationWarning)

    @gen.coroutine
    def run(self, on_finish, on_abort):
        # TODO: delegate to record.merge
        self._results = []
        for edge in self._in_edges:
            result = {"fields": edge.fields, "params": edge.params}
            if self.edge_type(edge) == "AsyncEdge":
                self.synchronizer(edge)
            while 1:
                if edge.status == "aborted":
                    on_abort()
                    return
                if edge.status == "done":
                    if self.edge_type(edge) == "IterEdge":
                        result["records"] = edge.records
                    elif self.edge_type(edge) == "FuncEdge":
                        result["records"] = map(edge.func, edge.records)
                    elif self.edge_type(edge) == "AsyncEdge":
                        result["records"] = self._rcds_tmp
                    break
                yield gen.sleep(self.interval)
            self._results.append(result)
        self.write(on_finish, on_abort)

    def interrupt(self):
        # TODO: core.workflow will call this when interrupted
        print("interrupt called")
        self.conn.interrupt()

    def on_submit(self):
        if os.path.exists(self.dest_path) and not self.allow_overwrite:
            raise InvalidOperationError("SQLite file already exists.")

    def add_in_edge(self, edge, port):
        if port != 0:
            raise InvalidOperationError("Invalid port")
        self._in_edges.append(edge)

    def out_edge(self, port):
        raise InvalidOperationError("Output node cannot have downstream edges")

    @gen.coroutine
    def synchronizer(self, edge):
        self._rcds_tmp = []
        while 1:
            in_ = yield edge.get()
            self._rcds_tmp.append(in_)

    @debug.profile
    def write(self, on_finish, on_abort):
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
            # Tables
            for result in self._results:
                # Create table
                phrases = []
                table_name = result["params"]["sqlite_schema"]["table"]
                for field in result["fields"]:
                    fieldphrase = field["key"]
                    if self.primary_key is not None and \
                            field["key"] == self.primary_key:
                        fieldphrase += " primary key check({} != '')".format(
                            self.primary_key)
                    fieldphrase += " collate nocase"
                    phrases.append(fieldphrase)
                fielddef = ", ".join(phrases)
                sql = "CREATE TABLE {}({})".format(table_name, fielddef)
                cur.execute(sql)
                # Insert records
                for i, rcd in enumerate(result["records"]):
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
        except KeyboardInterrupt:
            print("User cancel")
            self.conn.rollback()
            self.conn.close()
            on_abort()
        except:
            print(traceback.format_exc())
            self.conn.rollback()
            self.conn.close()
            on_abort()
        else:
            if self.schema_file:
                dest = self.dest_path.replace(".sqlite3", ".yaml")
                schema = []
                for result in self._results:
                    rsrc = result["params"]["sqlite_schema"]
                    rsrc["resourceType"] = "sqlite"
                    rsrc["fields"] = list(result["fields"])
                    schema.append(rsrc)
                with open(dest, "w") as f:
                    yaml.dump(schema, f)
            self.conn.commit()
            print("Cleaning up...")
            cur.execute("VACUUM")
            self.conn.close()
            on_finish()
