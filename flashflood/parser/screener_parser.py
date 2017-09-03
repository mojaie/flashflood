
from datetime import date


# Stubs
columnCount, rowCount, setBarcode, setDate, \
        setValues, setLayer, setRun, content, content_loader = [] * 9


options = {
    "cols": columnCount,
    "rows": rowCount
}

data = content_loader(content)


for plate in data["plates"]:
    setDate(plate.get("date", date.today().strftime("%y%m%d")))
    setBarcode(plate["plateId"])
    setValues(plate["wellValues"])
    setLayer(plate.get("layerIndex", 0))
    setRun(0)
