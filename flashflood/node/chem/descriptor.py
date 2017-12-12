#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import warnings

from flashflood import static
from flashflood.core.node import FuncNode, AsyncNode
from flashflood.core.task import UnexpectedOperationWarning


def mol_descriptors(descs, row):
    new_row = row.copy()
    for desc in descs:
        new_row[desc] = static.MOL_DESCS[desc].function(row["__molobj"])
    return new_row


class MolDescriptor(FuncNode):
    def __init__(self, descriptors, **kwargs):
        super().__init__(
            functools.partial(mol_descriptors, descriptors), **kwargs)
        if self.fields:
            warnings.warn(
                "Custom fields may overwrite MolDescriptor fields",
                UnexpectedOperationWarning)
        for desc in descriptors:
            props = static.MOL_DESCS[desc]
            self.fields.add({
                "key": desc, "name": props.name,
                props.format_type: props.format
            }, dupkey="skip")


class AsyncMolDescriptor(AsyncNode):
    def __init__(self, descriptors, **kwargs):
        super().__init__(**kwargs)
        self.descs = descriptors
        if self.fields:
            warnings.warn(
                "Custom fields may overwrite MolDescriptor fields",
                UnexpectedOperationWarning)
        for desc in descriptors:
            props = static.MOL_DESCS[desc]
            self.fields.add({
                "key": desc, "name": props.name,
                props.format_type: props.format
            }, dupkey="skip")

    def process_record(self, rcd):
        return mol_descriptors(self.descs, rcd)
