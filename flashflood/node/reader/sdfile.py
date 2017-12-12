#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from chorus import v2000reader
from chorus import molutil
from chorus.draw import calc2dcoords

from flashflood import static
from flashflood.node.reader.readerbase import ReaderBase


class SDFileReaderBase(ReaderBase):
    def __init__(self, sdf_options=(), implicit_hydrogen=False,
                 recalc_coords=False, **kwargs):
        super().__init__(**kwargs)
        self.sdf_options = sdf_options
        self.implicit_hydrogen = implicit_hydrogen
        self.recalc_coords = recalc_coords
        if not self.fields:
            self.fields.merge(
                {"key": s, "name": s, "format": "text"} for s in sdf_options)
        self.fields.add(static.MOLOBJ_FIELD)

    def run(self, on_finish, on_abort):
        self._out_edge.send(self.records_iter())
        on_finish()

    def records_iter(self):
        for mol in self.contents:
            row = {}
            if self.implicit_hydrogen:
                mol = molutil.make_Hs_implicit(mol)
            if self.recalc_coords:
                calc2dcoords.calc2dcoords(mol)
            row["__molobj"] = mol
            for op in self.sdf_options:
                row[op] = mol.data.get(op, "")
            yield row


class SDFileReader(SDFileReaderBase):
    def __init__(self, in_file, **kwargs):
        super().__init__(**kwargs)
        self.contents = v2000reader.mols_from_file(in_file)


class SDFileLinesInput(SDFileReaderBase):
    def __init__(self, lines, **kwargs):
        super().__init__(**kwargs)
        self.contents = v2000reader.mols_from_text(lines)
