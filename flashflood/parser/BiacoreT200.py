
import csv
from collections import deque


def file_loader(path, series, offset=0, sample=5):
    """Load sensorgram from BiacoreT200.
    TODO: multilayer

    Args:
        path: input file path
        sample: sampling frequency (datapoints / sample)

    Returns:
        dict of parsed data.
    """
    raw = []
    with open(path, encoding="shift-jis", newline="") as f:
        for row in csv.reader(f, delimiter="\t"):
            raw.append(row)
    rcds = []
    for r in raw[1::sample]:
        tv = deque(r[offset*2:])
        hdr = deque(series)
        while hdr:
            rcd = hdr.popleft().copy()
            t = tv.popleft()
            v = tv.popleft()
            if t and v:
                rcd.update({"time": float(t), "value": float(v)})
                rcds.append(rcd)
    return rcds


def equilibrium(path, sample=3):
    """Load equilibrium analysis data from BiacoreT200.
    TODO: multilayer

    Args:
        path: input file path
        sample: sampling frequency (datapoints / sample)

    Returns:
        dict of parsed data.
    """
    raw = deque()
    with open(path, encoding="shift-jis", newline="") as f:
        for row in csv.reader(f, delimiter="\t"):
            raw.append(row)
    rcds = []
    raw.popleft()
    for i, r in enumerate(raw):
        x, y, xfit, yfit = r
        if x and y:
            rcds.append({"type": "point", "conc": x, "value": y})
        if not i % sample:
            rcds.append({"type": "fitting", "conc": xfit, "value": yfit})
    return rcds


def screen_specs():
    """Vega plot format for SPR screening
    """
    return {
        "$schema": "https://vega.github.io/schema/vega/v3.0.json",
        "width": 180,
        "height": 180,
        "data": [{"name": "data"}],
        "scales": [
            {
                "name": "x",
                "type": "linear",
                "range": "width"
            },
            {
                "name": "y",
                "type": "linear",
                "range": "height"
            },
            {
                "name": "color",
                "type": "ordinal",
                "range": "category",
                "domain": {"data": "data", "field": "type"}
            }
        ],
        "axes": [
            {
                "orient": "bottom",
                "scale": "x",
                "title": "Time(sec)"
            },
            {
                "orient": "left",
                "scale": "y",
                "title": "Response unit(RU)"
            }
        ],
        "marks": [{
            "type": "group",
            "from": {
                "facet": {
                    "name": "series",
                    "data": "data",
                    "groupby": ["sensorgram"]
                }
            },
            "marks": [{
                "type": "line",
                "from": {"data": "series"},
                "encode": {
                    "enter": {
                        "x": {"scale": "x", "field": "time"},
                        "y": {"scale": "y", "field": "value"},
                        "stroke": {"scale": "color", "field": "type"},
                        "strokeWidth": {"value": 1}
                    }
                }
            }]
        }]
    }


def dose_specs():
    """Vega plot format for SPR dose-response
    """
    specs = screen_specs()
    marks = specs["marks"][0]
    marks["from"]["facet"]["groupby"] = ["conc"]
    marks["marks"][0]["encode"]["enter"]["stroke"]["field"] = "conc"
    specs["scales"][1]["zero"] = False
    specs["scales"][2]["type"] = "quantile"
    specs["scales"][2]["range"] = {"scheme": "blues", "count": 5}
    specs["scales"][2]["domain"]["field"] = "conc"
    return specs
