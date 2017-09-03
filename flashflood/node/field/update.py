#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood.node.function.apply import Apply


def rename(updates, row):
    new_row = row.copy()
    for old, new in updates.items():
        new_row[new] = new_row[old]
        del new_row[old]
    return new_row


class UpdateFields(Apply):
    def __init__(self, updates, fields=None, params=None):
        super().__init__(
            functools.partial(rename, updates),
            fields=fields, params=params
        )
        self.updates = updates

    def on_submitted(self):
        # TODO: Direct change on in_edge may cause unexpected behavior
        for field in self._in_edge.fields:
            if field["key"] in self.updates:
                field["key"] = self.updates[field["key"]]
        super().on_submitted()
