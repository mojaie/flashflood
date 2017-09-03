
import csv
from flashflood.parser import helper


def content_loader(lines):
    """Load LightCycler480 output file
    TODO: multilayer

    Args:
        path: input file path

    Returns:
        dict of parsed data
    """
    ro = 2  # row_offset
    raw = [row for row in csv.reader(lines, delimiter="\t")]
    parsed = {"plates": []}
    plate = {
        "plateId": raw[0][0].split("  ")[0].split(": ")[1],
        "layerIndex": 0,
        "wellValues": []
    }
    plate["wellValues"] = ["NaN"] * 384
    for row in raw[ro:]:
        idx = helper.well_index(row[2])
        try:
            value = float(row[5])
        except ValueError:
            continue
        plate["wellValues"][idx] = value
    parsed["plates"].append(plate)
    return parsed


def file_loader(path):
    with open(path, encoding="shift-jis", newline="") as f:
        results = content_loader(f)
    return results
