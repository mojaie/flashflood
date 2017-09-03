#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json

from chorus import v2000reader  # , v2000writer
from chorus import molutil
from chorus.draw import calc2dcoords

from flashflood import static
from flashflood.core.node import SyncNode


class SDFileInputBase(SyncNode):
    def __init__(self, sdf_options=(), implicit_hydrogen=False,
                 recalc_coords=False, fields=None, params=None):
        super().__init__()
        self.sdf_options = sdf_options
        self.implicit_hydrogen = implicit_hydrogen
        self.recalc_coords = recalc_coords
        self.fields.merge(
            {"key": s, "name": s, "valueType": "text"} for s in sdf_options)
        self.fields.add(static.MOLOBJ_FIELD)
        if fields is not None:
            self.fields.merge(fields)
        if params is not None:
            self.params.update(params)

    def records_iter(self):
        for mol in self.contents:
            row = {}
            if self.implicit_hydrogen:
                mol = molutil.make_Hs_implicit(mol)
            if self.recalc_coords:
                calc2dcoords.calc2dcoords(mol)
            row["_molobj"] = json.dumps(mol.jsonized())
            for op in self.sdf_options:
                row[op] = mol.data.get(op, "")
            yield row

    def on_submitted(self):
        self._out_edge.records = self.records_iter()
        self._out_edge.task_count = self.row_count
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self.params)


class SDFileInput(SDFileInputBase):
    def __init__(self, in_file, **kwargs):
        super().__init__(**kwargs)
        self.contents = v2000reader.mols_from_file(in_file)
        self.row_count = v2000reader.inspect_file(in_file)[1]


class SDFileLinesInput(SDFileInputBase):
    def __init__(self, lines, **kwargs):
        super().__init__(**kwargs)
        self.contents = v2000reader.mols_from_text(lines)
        self.row_count = v2000reader.inspect_text(lines)[1]
