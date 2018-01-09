#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from flashflood.interface import sqlite
from flashflood.node.reader.readerbase import ReaderBase


class SQLiteReader(ReaderBase):
    def __init__(self, tables, counter=None, **kwargs):
        super().__init__(**kwargs)
        self.tables = tables
        self.counter = counter

    def run(self, on_finish, on_abort):
        rcds = []
        for file_, table in self.tables:
            conn = sqlite.Connection(file_)
            rcds.append(conn.rows_iter(table))
            if self.counter is not None:
                self.counter.value += conn.rows_count(table)
        self._out_edge.send(itertools.chain(*rcds))
        on_finish()


class SQLiteReaderSearch(SQLiteReader):
    def __init__(self, tables, key, values, **kwargs):
        super().__init__(tables, **kwargs)
        self.key = key
        self.values = values

    def run(self, on_finish, on_abort):
        rcds = []
        for v in self.values:
            for file_, table in self.tables:
                conn = sqlite.Connection(file_)
                if self.key not in conn.columns(table):
                    continue
                found = conn.find_first(table, self.key, v)
                if found:
                    rcds.append(found)
                    break
            else:
                rcds.append({self.key: v})
        if self.counter is not None:
            self.counter.value += len(self.values)
        self._out_edge.send(rcds)
        on_finish()


class SQLiteReaderFilter(SQLiteReader):
    def __init__(self, tables, key, value, operator, **kwargs):
        super().__init__(tables, **kwargs)
        self.key = key
        self.value = value
        self.operator = operator

    def run(self, on_finish, on_abort):
        rcds = []
        for file_, table in self.tables:
            conn = sqlite.Connection(file_)
            if self.key not in conn.columns(table):
                continue
            found = conn.find_all(table, self.key, self.value, self.operator)
            if self.counter is not None:
                found = list(found)
                self.counter.value += len(found)
            rcds.append(found)
        self._out_edge.send(itertools.chain(*rcds))
        on_finish()
