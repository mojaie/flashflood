
import csv


def content_loader(lines):
    """Load datafile from SpectraMax M2.
    TODO: multilayer

    Args:
        lines: lines of input file (or file object)

    Returns:
        parsed data
    """
    bs = 20  # block_size
    bo = 1  # block_offset
    pr = 16  # plate_rows
    ro = 2  # row_offset
    pc = 24  # plate_cols
    co = 2  # col_offset

    raw = [row for row in csv.reader(lines, delimiter="\t")]
    blocks = [raw[i*bs+bo:(i + 1)*bs+bo] for i in range(len(raw) // bs)]
    parsed = {"plates": []}
    for block in blocks:
        plate = {}
        plate["plateId"] = block[0][1]
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
    return parsed


def file_loader(path):
    with open(path, encoding="shift-jis", newline="") as f:
        results = content_loader(f)
    return results
