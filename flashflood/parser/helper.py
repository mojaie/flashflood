
from chorus.util import iterator as ci


def data_to_record(data, settings):
    """
    settings = {
        "plate": "time",
        "column": "conc1",
        "colLabels": [0.2, 0.1, 0.05, 0],
        "colRepeats": 2,
        "colOffset": 1,
        "row": "conc2",
        "rowLabels": [40, 20, 10, 5, 2.5, 1.25, 0.625, 0],
        "rowRepeats": 1,
        "rowOffset": 1
    }
    """
    rcds = []
    for plate in data["plates"]:
        cols = ci.repeat_each(
            settings["colLabels"], settings.get("colRepeats", 1))
        for i, cl in enumerate(cols):
            i_at = i + settings.get("colOffset", 0)
            rows = ci.repeat_each(
                settings["rowLabels"], settings.get("rowRepeats", 1))
            for j, rl in enumerate(rows):
                j_at = j + settings.get("rowOffset", 0)
                rcd = {}
                rcd[settings["column"]] = cl
                rcd[settings["row"]] = rl
                rcd["value"] = plate["wellValues"][j_at * 24 + i_at]
                rcd[settings["plate"]] = plate["plateId"]
                rcds.append(rcd)
    return rcds


def well_index(position):
    row = ord(position[0].upper()) - 65
    col = int(position[1:]) - 1
    return row * 24 + col
