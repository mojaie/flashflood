#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import copy

from flashflood.lod import ListOfDict


class Container(object):
    """Data container"""
    def __init__(self):
        self.records = []
        self.fields = ListOfDict()
        self.params = {}


class Counter(object):
    """Count container"""
    def __init__(self):
        self.value = 0


class Sampler(object):
    """Data sampler"""
    def __init__(self, size=5, frequency=1, deep_copy=False):
        self.records = []
        self.size = size
        self.frequency = frequency
        self.deep_copy = deep_copy
        self.done = False
        self._source_count = 0

    def put(self, record):
        if self.done:
            return
        if len(self.records) >= self.size:
            self.done = True
            return
        if not self._source_count % self.frequency:
            if self.deep_copy:
                copied = copy.deepcopy(record)
            else:
                copied = copy.copy(record)
            self.records.append(copied)
        self._source_count += 1

    def put_from_list(self, records):
        if not isinstance(records, list):
            raise ValueError("Source records should be a list.")
        source = iter(records)
        while not self.done:
            try:
                s = next(source)
            except StopIteration:
                return
            self.put(s)
