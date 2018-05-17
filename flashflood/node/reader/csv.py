#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import csv

from flashflood.node.reader.readerbase import ReaderBase


class CsvReader(ReaderBase):
    def __init__(self, in_file, delimiter=",", **kwargs):
        super().__init__(**kwargs)
        self.in_file = in_file
        self.delimiter = delimiter
        if not self.fields:
            with open(self.in_file, newline="") as f:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                try:
                    next(reader)
                except StopIteration:
                    pass
                self.fields.merge(
                    {"key": f, "name": f, "format": "raw"}
                    for f in reader.fieldnames
                )

    def run(self, on_finish, on_abort):
        self._out_edge.send(self.reader())
        on_finish()

    def reader(self):
        with open(self.in_file, newline="") as f:
            for row in csv.DictReader(f, delimiter=self.delimiter):
                yield row
