
import csv


def content_loader(lines):
    """Load datafile from Envision.

    Args:
        path: input file path

    Returns:
        dict of parsed data.
    """
    pr = 16  # plate_rows
    ro = 9  # row_offset
    pc = 24  # plate_cols
    co = 0  # col_offset
    blocks = []
    buf = []
    for row in csv.reader(lines, delimiter=","):
        if len(buf) and len(row) and row[0].startswith("Plate information"):
            blocks.append(buf)
            buf = []
        buf.append(row)
    blocks.append(buf)
    prev_barcode = None
    prev_layer = 0
    parsed = {"plates": []}
    for block in blocks:
        plate = {}
        plate["plateId"] = block[2][2]
        if plate["plateId"] == prev_barcode:
            plate["layerIndex"] = prev_layer + 1
        else:
            plate["layerIndex"] = 0
        plate["wellValues"] = []
        for row in block[ro:ro+pr]:
            for cell in row[co:co+pc]:
                try:
                    value = float(cell)
                except ValueError:
                    value = "NaN"
                plate["wellValues"].append(value)
        parsed["plates"].append(plate)
        prev_barcode = plate["plateId"]
        prev_layer = plate["layerIndex"]
    return parsed


def file_loader(path):
    with open(path, encoding="UTF-8", newline="") as f:
        results = content_loader(f.read())
    return results
