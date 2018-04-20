#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import csv

from tornado import gen

from flashflood.core.node import Node
from flashflood.core.task import InvalidOperationError


class CsvWriter(Node):
    def __init__(self, dest_path, keys, allow_overwrite=True, **kwargs):
        super().__init__(**kwargs)
        self.dest_path = dest_path
        self.keys = keys
        self.allow_overwrite = allow_overwrite
        self._records = []

    @gen.coroutine
    def run(self, on_finish, on_abort):
        if self.edge_type(self._in_edge) == "AsyncEdge":
            self.synchronizer()
        while 1:
            if self._in_edge.status == "aborted":
                on_abort()
                break
            if self._in_edge.status == "done":
                if self.edge_type(self._in_edge) == "IterEdge":
                    self._records = list(self._in_edge.records)
                elif self.edge_type(self._in_edge) == "FuncEdge":
                    self._records = list(
                        map(self._in_edge.func, self._in_edge.records))
                on_finish()
                break
            yield gen.sleep(self.interval)
        self.write(on_finish)

    def out_edge(self, port):
        raise InvalidOperationError("Output node cannot have downstream edges")

    def on_submit(self):
        if os.path.exists(self.dest_path) and not self.allow_overwrite:
            raise InvalidOperationError("The file already exists.")

    @gen.coroutine
    def synchronizer(self):
        while 1:
            in_ = yield self._in_edge.get()
            self._records.append(in_)

    def write(self, on_finish):
        with open(self.dest_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.keys)
            writer.writeheader()
            for row in self._records:
                writer.writerow(row)
        on_finish()
