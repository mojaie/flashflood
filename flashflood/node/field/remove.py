#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood.core.node import FuncNode


def remove(key, row):
    new_row = row.copy()
    del new_row[key]
    return new_row


class RemoveField(FuncNode):
    def __init__(self, key, **kwargs):
        super().__init__(functools.partial(remove, key), **kwargs)
        self.key = key

    def merge_fields(self):
        super().merge_fields()
        self._out_edge.fields.delete("key", self.key)


def remove_many(keys, row):
    new_row = row.copy()
    for k in keys:
        del new_row[k]
    return new_row


class RemoveFields(FuncNode):
    def __init__(self, keys, **kwargs):
        super().__init__(functools.partial(remove_many, keys), **kwargs)
        self.keys = keys

    def merge_fields(self):
        super().merge_fields()
        for k in self.keys:
            self._out_edge.fields.delete("key", k)


def retain(keys, row):
    new_row = {}
    for k in keys:
        new_row[k] = row[k]
    return new_row


class RetainFields(FuncNode):
    def __init__(self, keys, **kwargs):
        super().__init__(functools.partial(retain, keys), **kwargs)
        self.keys = keys

    def merge_fields(self):
        for k in self.keys:
            self._out_edge.fields.add(self._in_edge.fields.find("key", k))
        self._out_edge.fields.merge(self.fields)
