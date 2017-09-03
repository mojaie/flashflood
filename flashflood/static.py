
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


INDEX_FIELD = {"key": "_index", "name": "index", "valueType": "count"}
COMPID_FIELD = {"key": "compound_id", "name": "compound ID",
                "valueType": "compound_id"}
NAME_FIELD = {"key": "name", "name": "Name", "valueType": "text"}
MOLOBJ_FIELD = {"key": "_molobj", "name": "Molecule object",
                "valueType": "json"}

CHEM_FIELDS = ListOfDict([
    MOLOBJ_FIELD,
    {"key": "_structure", "name": "Structure", "valueType": "svg"},
    {"key": "_mw", "name": "MW", "valueType": "numeric"},
    {"key": "_mw_wo_sw", "name": "MW w/o salt and water",
     "valueType": "numeric"},
    {"key": "_formula", "name": "Formula", "valueType": "text"},
    {"key": "_logp", "name": "WCLogP", "valueType": "numeric"},
    {"key": "_nonH", "name": "Non-H atom count", "valueType": "numeric"}
])

CHEM_FUNCTIONS = {
    "_structure": mol_to_svg,
    "_mw": molutil.mw,
    "_mw_wo_sw": molutil.mw_wo_sw,
    "_formula": molutil.formula,
    "_logp": wclogp.wclogp,
    "_nonH": molutil.non_hydrogen_count
}
