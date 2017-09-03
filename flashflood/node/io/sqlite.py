#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.sqliteconnection import Connection
from flashflood.sqlitehelper import SQLITE_HELPER as sq
from flashflood.core.node import SyncNode


class SQLiteInput(SyncNode):
    def __init__(self, query, params=None):
        super().__init__()
        self.query = query
        self.fields = sq.resource_fields(query["targets"])
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        self._out_edge.records = sq.records_iter(self.query["targets"])
        self._out_edge.task_count = sq.record_count(self.query["targets"])
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self.params)


class SQLiteSearchInput(SQLiteInput):
    def on_submitted(self):
        self._out_edge.records = (
            sq.search(self.query["targets"], self.query["key"], v)
            for v in self.query["values"]
        )
        self._out_edge.task_count = sq.record_count(self.query["targets"])
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self.params)


class SQLiteFilterInput(SQLiteInput):
    def on_submitted(self):
        self._out_edge.records = sq.find_all(
            self.query["targets"], self.query["key"],
            self.query["values"], self.query["operator"],
            fields=self.query.get("fields")
        )
        self._out_edge.task_count = sq.record_count(self.query["targets"])
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self.params)


class SQLiteCustomQueryInput(SyncNode):
    def __init__(self, sql, table, fields=None):
        super().__init__()
        self.sql = sql
        self.table = table
        if fields is None:
            self.fields = []
        else:
            self.fields = fields

    def on_submitted(self):
        conn = Connection()
        self._out_edge.records = conn.fetch_iter(self.sql)
        self._out_edge.task_count = conn.rows_count(self.table)
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self.params)
