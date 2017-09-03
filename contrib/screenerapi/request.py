#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json

from tornado import httpclient

from flashflood import configparser as conf


def request(query, auth_header):
    rsrc = conf.schema()["resources"].find("key", query["dest"])
    url = "{}{}".format(rsrc["url"], query)
    print("HTTP Request: {}".format(url))
    http_client = httpclient.HTTPClient()
    request = httpclient.HTTPRequest(url)
    request.headers = {"Authorization": auth_header}
    # request.auth_username = user
    # request.auth_password = password
    response = http_client.fetch(request)
    http_client.close()
    return response.body


def qcs_info(qcs_refids, auth_header):
    queries = " OR ".join(["qcsRefId%3A{}".format(q) for q in qcs_refids])
    res = request("/qcSessions?q={}".format(queries), auth_header)
    obj = json.loads(res.decode())
    return obj["qcSessions"]


def compound(compound_id):
    queries = " OR ".join(["qcsRefId%3A{}".format(q) for q in qcs_refids])
    res = request("/qcSessions?q={}".format(queries), auth_header)
    obj = json.loads(res.decode())
    return obj["qcSessions"]


def layer_values(qcs_refid, plate_idx, auth_header):
    res = request("/plates?qcsRefIds={}&q=plateIndex%3A{}\
&fields=layerIndex%2Cwells.rawValues%2Cwells.compoundIds".format(
        qcs_refid, plate_idx), auth_header)
    obj = json.loads(res.decode())
    arrays = {}
    for p in obj["plates"]:
        idx = p["layerIndex"]
        keys = p["wells"]["compoundIds"]
        values = p["wells"]["rawValues"]
        arrays[idx] = zip(keys, values)
    return arrays


def plate_values(qcs_refid, layer_idx, auth_header):
    res = request("/plates?qcsRefIds={}&layerIndices={}\
&fields=layerIndex%2Cwells.rawValues%2Cwells.compoundIds".format(
        qcs_refid, layer_idx), auth_header)
    obj = json.loads(res.decode())
    keys = []
    values = []
    for p in obj["plates"]:
        keys.extend(p["wells"]["compoundIds"])
        values.extend(p["wells"]["rawValues"])
    rows_gen = zip(keys, values)
    return rows_gen


def plate_stats(qcs_refid, layer_idx, auth_header):
    res = request("/plates?qcsRefIds={}&layerIndices={}\
&fields=barcode%2ClayerIndex%2CzPrime%2CwellTypes".format(
        qcs_refid, layer_idx), auth_header)
    obj = json.loads(res.decode())
    stat_cols = [
        {"key": "Plate", "visible": True},
        {"key": "Low control: Mean", "visible": True},
        {"key": "Low control: SD", "visible": True},
        {"key": "Low control: CV", "visible": True},
        {"key": "High control: Mean", "visible": True},
        {"key": "High control: SD", "visible": True},
        {"key": "High control: CV", "visible": True},
        {"key": "S/B", "visible": True},
        {"key": "Z'", "visible": True}
    ]
    stat_rcds = []
    for p in obj["plates"]:
        row = {"Plate": p.get("barcode", "<no barcode>")}
        both = 0
        if "NEUTRAL_CONTROL" in p["wellTypes"]:
            low_mean = p["wellTypes"]["NEUTRAL_CONTROL"]["mean"]
            low_sd = p["wellTypes"]["NEUTRAL_CONTROL"]["sd"]
            row["Low control: Mean"] = round(low_mean, 3)
            row["Low control: SD"] = round(low_sd, 3)
            row["Low control: CV"] = round(low_sd / low_mean * 100, 1)
            both += 1
        if "INHIBITOR_CONTROL" in p["wellTypes"]:
            high_mean = p["wellTypes"]["INHIBITOR_CONTROL"]["mean"]
            high_sd = p["wellTypes"]["INHIBITOR_CONTROL"]["sd"]
            row["High control: Mean"] = round(high_mean, 3)
            row["High control: SD"] = round(high_sd, 3)
            row["High control: CV"] = round(high_sd / high_mean * 100, 1)
            both += 1
        if both == 2:
            row["S/B"] = round(low_mean / high_mean, 2)
            row["Z'"] = round(p["zPrime"], 3)
        stat_rcds.append(row)
    return {"columns": stat_cols, "records": stat_rcds}
