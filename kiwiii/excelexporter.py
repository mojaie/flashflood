#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import base64
import io
import json
import re
import multiprocessing as mp
import os
import pickle

from xlsxwriter.workbook import Workbook

from chorus.model.graphmol import Compound
from chorus.draw.matplotlib import Matplotlib

from kiwiii import tablecolumn as tc
from kiwiii.util import lod

MOL = tc.MolObjectColumn()
STRUCT = tc.StructureColumn()

EXPORT_OPTIONS = {
    "in_memory": {"in_memory": True},
    "worksheet_name": "sheet1",
    "default_row_height": 20,  # Point? (Depends on device)
    "struct_row_height": 180,  # Point? (Depends on device)
    "header_row_height": 20,  # Point? (Depends on device)
    "default_col_width": 16,  # Width of a character? (Depends on device)
    "text_align": {"align": "center", "valign": "vcenter"},
    "img_options": {"x_offset": 7.5,  # Point? (Depends on device)
                    "y_offset": 7.5,  # Point? (Depends on device)
                    "positioning": 1}
}


def json_to_xlsx(data, opts=EXPORT_OPTIONS):
    """Export dataframe to Microsoft Excel worksheet(.xlsx).

    This method uses Qt as SVG->PNG rendering engine.
    It is recommended to execute this function in subprocess to avoid seg fault
    unless the situation that Qt GUI thread is always running in background.
    see kiwiii.util.multiprocess

    Args:
      json: JSON table object
      opt: export setting dict

    Returns:
      io.BytesIO
    """
    buf = io.BytesIO()
    wb = Workbook(buf, opts["in_memory"])
    text_format = wb.add_format(opts["text_align"])
    for table in data["tables"]:
        sheet_name = re.sub(r"[\[\]\:\*\?\/\\]", "_", table["name"])
        sheet = wb.add_worksheet(sheet_name)
        # TODO: appropriate row height
        struct = lod.find("key", STRUCT.key, table["columns"])
        if struct is not None and struct["visible"]:
            sheet.set_default_row(opts["struct_row_height"])
        else:
            sheet.set_default_row(opts["default_row_height"])
        sheet.set_row(0, opts["header_row_height"])
        i = 0
        for col in table["columns"]:
            if not col["visible"]:
                continue
            if "name" not in col:
                col["name"] = col["key"]
            sheet.write(0, i, col["name"], text_format)
            col_width = [opts["default_col_width"]]
            for j, row in enumerate(table["records"]):
                if col["key"] == STRUCT.key:
                    mol = Compound(json.loads(row[MOL.key]))
                    mpl = Matplotlib(mol)
                    size = opts["struct_row_height"] - opts[
                        "img_options"]["y_offset"] * 2
                    factor = size / mpl.get_size()[1] * mpl.dpi / 72
                    img_opts = {"image_data": mpl.to_bytesio(),
                                "x_scale": factor, "y_scale": factor}
                    img_opts.update(opts["img_options"])
                    sheet.insert_image(j+1, i, "{}_{}.png".format(i, j),
                                       img_opts)
                    col_width.append(
                        (opts["struct_row_height"] / 8.43 + 3) * 1.33)
                    # a 1.33 is pixel/point conversion factor
                    # a 8.43 is pixel/character width conversion factor
                    # +3 for a bug in image placement of Mac Excel
                elif col.get("valueType") == "image":
                    data = row.get(col["key"], "")
                    if not data:
                        continue
                    bytestr = base64.b64decode(data.split(",")[1])
                    img_opts = {"image_data": io.BytesIO(bytestr),
                                "x_scale": 1, "y_scale": 1}
                    img_opts.update(opts["img_options"])
                    sheet.insert_image(j+1, i, "{}_{}.png".format(i, j),
                                       img_opts)
                    col_width.append((180 / 8.43 + 3) * 1.33)
                else:
                    sheet.write(j + 1, i, row.get(col["key"], ""),
                                text_format)
            sheet.set_column(i, i, max(col_width))
            i += 1
    wb.close()
    return buf


class FileBasedIPC(object):
    """ Windows compatible file-based IPC
    """
    def __init__(self, func):
        self.func = func

    def _f(self, *args, **kwargs):
        tmp_name = "_tmp.pickle"
        argsl = list(args)
        argsl.append(tmp_name)
        proc = mp.Process(target=self._p, args=argsl, kwargs=kwargs)
        proc.start()
        proc.join()
        with open(tmp_name, "rb") as f:
            result = pickle.load(f)
        os.remove(tmp_name)
        return result

    def _p(self, *args, **kwargs):
        al = list(args)
        n = al.pop()
        res = self.func(*al, **kwargs)
        with open(n, "wb") as f:
            pickle.dump(res, f)

    def __call__(self):
        return self._f
