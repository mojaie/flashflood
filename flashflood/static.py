
import collections

from tornado import process
from chorus import molutil, wclogp
from chorus.draw.svg import mol_to_svg

from flashflood.lod import ListOfDict

JOB_RESULT_SCHEMA = "https://mojaie.github.io/flashflood/_static/specs/job_result_v1.0.json"

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

# TODO: move to server_config?
INDEX_FIELD = {"key": "index", "name": "Index", "d3_format": "d"}
COMPID_FIELD = {"key": "compound_id", "name": "Compound ID",
                "format": "compound_id"}
NAME_FIELD = {"key": "name", "name": "Name", "format": "text"}
MOLJSON_FIELD = {"key": "__moljson", "name": "Molecule JSON", "format": "json"}
MOLPICKLE_FIELD = {"key": "__molpickle", "name": "Pickled molecule",
                   "format": "pickle"}
MOLOBJ_FIELD = {"key": "__molobj", "name": "Molecule object",
                "format": "pyobject"}


# TODO: move to node.chem.descriptor?
Descriptor = collections.namedtuple(
    "Descriptor", ("function", "name", "format_type", "format"))

MOL_DESCS = collections.OrderedDict([
    ("structure", Descriptor(mol_to_svg, "Structure", "format", "svg")),
    ("_mw", Descriptor(molutil.mw, "MW", "d3_format", ".2f")),
    ("_mw_wo_sw", Descriptor(molutil.mw_wo_sw, "MW w/o salt and water",
                             "d3_format", ".2f")),
    ("_formula", Descriptor(molutil.formula, "Formula", "format", "text")),
    ("_logp", Descriptor(wclogp.wclogp, "WCLogP", "d3_format", ".1f")),
    ("_nonH", Descriptor(molutil.non_hydrogen_count, "Non-H atom count",
                         "d3_format", "d"))
])

MOL_DESC_KEYS = list(MOL_DESCS.keys())  # OrderedDict.key is not picklable
MOL_DESC_FIELDS = ListOfDict({
    "key": key, "name": desc.name, desc.format_type: desc.format
} for key, desc in MOL_DESCS.items())
