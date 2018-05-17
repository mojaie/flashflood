#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.core.node import IterNode


class Unpack(IterNode):
    """ Generate rows from a list field.

    Args:
        source: source field key
        dist: dist field key
    """
    def __init__(self, source, dist, **kwargs):
        super().__init__(**kwargs)
        self.source = source
        self.dist = dist
        self._mapping = {}

    def merge_fields(self):
        super().merge_fields()
        if self.source != self.dist:
            self._out_edge.fields.delete("key", self.source)

    def processor(self, rcds):
        for r in rcds:
            for e in r[self.source]:
                new_r = r.copy()
                new_r[self.dist] = e
                if self.source != self.dist:
                    del new_r[self.source]
                yield new_r
