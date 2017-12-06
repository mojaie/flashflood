
from tornado import process
from chorus import molutil, wclogp
from chorus.draw.svg import SVG

from flashflood.lod import ListOfDict


SCHEMA_VERSION = 0.8

""" Option module availability """

try:
    from chorus import rdkit
    RDK_AVAILABLE = True
except ImportError:
    RDK_AVAILABLE = False
try:
    import cython
    NUMERIC_MODULE = "Cython"
except ImportError:
    try:
        import numexpr
        NUMERIC_MODULE = "NumExpr"
    except ImportError:
        NUMERIC_MODULE = "Numpy"


""" Server status """

PROCESSES = process.cpu_count()


""" Field definition """


def mol_to_svg(mol):
    return SVG(mol).contents()


INDEX_FIELD = {"key": "index", "name": "index", "d3_format": "d"}
COMPID_FIELD = {"key": "compound_id", "name": "compound ID",
                "format": "compound_id"}
NAME_FIELD = {"key": "name", "name": "Name", "format": "text"}
MOLOBJ_FIELD = {"key": "__molobj", "name": "Molecule object", "format": "json"}

CHEM_FIELDS = ListOfDict([
    MOLOBJ_FIELD,
    {"key": "structure", "name": "Structure", "format": "svg"},
    {"key": "_mw", "name": "MW", "d3_format": ".2f"},
    {"key": "_mw_wo_sw", "name": "MW w/o salt and water", "d3_format": ".2f"},
    {"key": "_formula", "name": "Formula", "format": "text"},
    {"key": "_logp", "name": "WCLogP", "d3_format": ".1f"},
    {"key": "_nonH", "name": "Non-H atom count", "d3_format": "d"}
])

CHEM_FUNCTIONS = {
    "structure": mol_to_svg,
    "_mw": molutil.mw,
    "_mw_wo_sw": molutil.mw_wo_sw,
    "_formula": molutil.formula,
    "_logp": wclogp.wclogp,
    "_nonH": molutil.non_hydrogen_count
}
