#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.node.reader.readerbase import ReaderBase
from flashflood.sqlitehelper import SQLITE_HELPER as sq


class SQLiteReader(ReaderBase):
    def __init__(self, query, counter=None, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.counter = counter
        self.fields.merge(sq.resource_fields(query["targets"]))

    def run(self, on_finish, on_abort):
        if self.counter is not None:
            self.counter.value = sq.record_count(self.query["targets"])
        self._out_edge.send(sq.records_iter(self.query["targets"]))
        on_finish()


class SQLiteReaderSearch(SQLiteReader):
    def run(self, on_finish, on_abort):
        self._out_edge.send(
            sq.search(self.query["targets"], self.query["key"], v)
            for v in self.query["values"]
        )
        on_finish()


class SQLiteReaderFilter(SQLiteReader):
    def run(self, on_finish, on_abort):
        self._out_edge.send(sq.find_all(
            self.query["targets"], self.query["key"],
            self.query["values"], self.query["operator"],
            fields=self.query.get("fields")
        ))
        on_finish()
