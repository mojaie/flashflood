#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from flashflood import static
from flashflood.core.node import FunctionNode
from flashflood.node.function.apply import AsyncApply


def mol_descriptors(descs, row):
    new_row = row.copy()
    for desc in descs:
        new_row[desc] = static.MOL_DESCS[desc].function(row["__molobj"])
    return new_row


class MolDescriptor(FunctionNode):
    def __init__(self, descriptors, params=None):
        super().__init__(
            functools.partial(mol_descriptors, descriptors),
            params=params)
        fields = []
        for desc in descriptors:
            props = static.MOL_DESCS[desc]
            fields.append({
                "key": desc, "name": props.name,
                props.format_type: props.format
            })
        self.fields.merge(fields)


class AsyncMolDescriptor(AsyncApply):
    def __init__(self, descriptors, params=None):
        super().__init__(
            functools.partial(mol_descriptors, descriptors),
            params=params)
        fields = []
        for desc in descriptors:
            props = static.MOL_DESCS[desc]
            fields.append({
                "key": desc, "name": props.name,
                props.format_type: props.format
            })
        self.fields.merge(fields)
