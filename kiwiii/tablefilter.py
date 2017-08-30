#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools
import json
import operator
import os
import pickle
import re
import traceback
import yaml

from chorus import mcsdr, molutil, substructure, smilessupplier, v2000reader
from chorus.draw import calc2dcoords
from chorus.model.graphmol import Compound
from chorus.draw.svg import SVG

from kiwiii.sqliteconnection import Connection
from kiwiii import tablecolumn as tc
from kiwiii.util import lod

CYTHON_AVAILABLE = mcsdr.CYTHON_AVAILABLE
NUMEXPR_AVAILABLE = mcsdr.NUMEXPR_AVAILABLE
try:
    from chorus import rdkit
    RDK_AVAILABLE = True
    print("RDKit is available")
except ImportError:
    RDK_AVAILABLE = False
    print("RDKit is not available")

MOL = tc.MolObjectColumn()
SIM = tc.SimilarityColumn()
MCS = tc.McsSizeColumn()
MCSDR = tc.McsdrSizeColumn()

with open(os.path.join(
        os.path.dirname(__file__), "../server_config.yaml")) as f:
    config = yaml.load(f.read())


def like_operator(a, b):
    """ regexp implementation of sqlite LIKE operator """
    return re.match(b.replace("%", ".*?").replace("_", "[\w ]"), a) is not None


class DBSearchFilter(object):
    def __init__(self, q):
        self.query = q
        self.founds = []

    def _resources_iter(self):
        dbconns = {}
        for trgt in self.query["targets"]:
            db, table = trgt.split(':')
            if db not in dbconns:
                path = os.path.join(config["data_dir"],
                                    "{}.sqlite3".format(db))
                dbconns[db] = Connection(path)
            conn = dbconns[db]
            tbl = lod.find("entity", trgt, conn.document()["tables"])
            tbl["table"] = table
            yield conn, tbl

    def _update_founds(self, table_id):
        if table_id not in self.founds:
            self.founds.append(table_id)

    def initialize(self, data):
        data["dataSource"] = []
        data["format"] = "datatable"
        dbconns = {}
        search_cnt = []
        for trgt in self.query["targets"]:
            db, table = trgt.split(':')
            if db not in dbconns:
                path = os.path.join(config["data_dir"],
                                    "{}.sqlite3".format(db))
                dbconns[db] = Connection(path)
            conn = dbconns[db]
            search_cnt.append(conn.rows_count((table,)))
        data["searchCount"] = sum(search_cnt)


def dbquery_processor(row, arg):
    row.update(arg)
    return True


class FirstMatchFilter(DBSearchFilter):
    def __init__(self, *args):
        super().__init__(*args)
        self.row_processor = dbquery_processor

    def _is_invalid_key(self, cols):
        if self.query["key"] == "ID":
            return False
        for col in cols:
            if col.get("dataColumn") == self.query["key"]:
                return False
            if col["key"] == self.query["key"]:
                return False
        return True

    def _null_record(self, key, value):
        return {self.query["key"]: value}

    def args_generator(self):
        for val in self.query["values"]:
            for conn, tbl in self._resources_iter():
                if self._is_invalid_key(tbl["columns"]):
                    continue
                res = conn.find_first(self.query["key"], (val,),
                                      (tbl["table"],))
                if res is not None:
                    row = dict(res)
                    row["source"] = tbl["id"]
                    self._update_founds(tbl["id"])
                    yield row
                    break
            else:
                yield self._null_record(self.query["key"], val)

    def initialize(self, data):
        super().initialize(data)
        data["query"] = {
            "targets": self.query["targets"],
            "key": self.query["key"],
            "values": self.query["values"],
            "operator": "fm"
        }
        data["dataSource"] = self.founds


class FindAllFilter(FirstMatchFilter):
    def __init__(self, *args):
        super().__init__(*args)

    def args_generator(self):
        op = {"eq": "=", "gt": ">", "lt": "<", "ge": ">=", "le": "<=",
              "lk": "LIKE", "in": "IN"}[self.query["operator"]]
        for conn, tbl in self._resources_iter():
            if self._is_invalid_key(tbl["columns"]):
                continue
            row = None  # for empty check
            for res in conn.find_iter(self.query["key"], self.query["values"],
                                      (tbl["table"],), op):
                row = dict(res)
                row["source"] = tbl["id"]
                yield row
            if row is not None:
                self._update_founds(tbl["id"])

    def initialize(self, data):
        super().initialize(data)
        data["query"]["operator"] = self.query["operator"]


def chemquery_processor(row, arg):
    mol = Compound(pickle.loads(arg[MOL.key]))
    row[MOL.key] = json.dumps(mol.jsonized())
    del arg[MOL.key]
    row.update(arg)
    return True


class ChemFirstMatchFilter(FirstMatchFilter):
    def __init__(self, *args):
        super().__init__(*args)
        self.row_processor = chemquery_processor

    def _is_invalid_key(self, cols):
        return lod.find("key", self.query["key"], cols) is None

    def _null_record(self, key, value):
        null_record = {key: value}
        null_record[MOL.key] = pickle.dumps(
            molutil.null_molecule().jsonized())
        return null_record


class ChemFindAllFilter(FindAllFilter):
    def __init__(self, *args):
        super().__init__(*args)
        self.row_processor = chemquery_processor

    def is_invalid_key(self, cols):
        return lod.find("key", self.query["key"], cols) is None


def chemprop_processor(func, sort, key, vals, op, row, arg):
    sortfunc = {"numeric": float, "text": str}
    vs = [sortfunc[sort](v) for v in vals]
    v = {True: vs, False: vs[0]}[op == "in"]
    opfunc = {
        "eq": operator.eq, "gt": operator.gt, "lt": operator.lt,
        "ge": operator.ge, "le": operator.le,
        "lk": like_operator, "in": lambda a, b: a in b
    }[op]
    mol = Compound(pickle.loads(arg[MOL.key]))
    res = func(mol)
    try:
        valid = opfunc(res, v)
    except TypeError as e:
        print(e, res, v)
        return False
    if valid:
        row[MOL.key] = json.dumps(mol.jsonized())
        del arg[MOL.key]
        row.update(arg)
        row[key] = res
        return True
    return False


class ChemPropFilter(ChemFindAllFilter):
    def __init__(self, *args):
        super().__init__(*args)
        keyobj = next(filter(
                lambda e: e.key == self.query["key"], tc.CALC_COLS))
        self.row_processor = functools.partial(
            chemprop_processor, keyobj.function, keyobj.sort,
            self.query["key"], self.query["values"], self.query["operator"])

    def args_generator(self):
        for conn, tbl in self._resources_iter():
            for res in conn.rows_iter((tbl["table"],)):
                row = dict(res)
                row["source"] = tbl["id"]
                yield row

    def on_task_done(self, res, data):
        if res is not None:
            data["records"].append(res)
            self._update_founds(res["source"])
            data["dataSource"] = self.founds


def substr_processor(func, qmol, row, arg):
    mol = Compound(pickle.loads(arg[MOL.key]))
    if func(mol, qmol):
        row[MOL.key] = json.dumps(mol.jsonized())
        del arg[MOL.key]
        row.update(arg)
        return True
    return False


def parse_chem_query(data, fmt, source):
    if fmt == "smiles":
        try:
            qmol = smilessupplier.smiles_to_compound(data)
            calc2dcoords.calc2dcoords(qmol)
        except (ValueError, StopIteration):
            raise TypeError()
    elif fmt == "molfile":
        try:
            qmol = v2000reader.mol_from_text(data)
        except (ValueError, StopIteration):
            raise TypeError()
    elif fmt == "dbid":
        db, tbl = source.split(":")
        path = os.path.join(config["data_dir"], "{}.sqlite3".format(db))
        conn = Connection(path)
        res = conn.find_first("ID", (data,), (tbl,))
        if res is None:
            raise ValueError()
        qmol = Compound(pickle.loads(res[MOL.key]))
    return qmol


class SubStructFilter(DBSearchFilter):
    def __init__(self, *args):
        super().__init__(*args)
        self.qmol = parse_chem_query(
            self.query["value"], self.query["format"],
            self.query.get("querySource"))
        self.name = "Substructure"
        func = functools.partial(substructure.substructure,
                                 ignore_hydrogen=self.query["ignoreHs"])
        self.row_processor = functools.partial(
            substr_processor, func, self.qmol)

    def args_generator(self):
        for conn, tbl in self._resources_iter():
            for res in conn.rows_iter((tbl["table"],)):
                row = dict(res)
                row["source"] = tbl["id"]
                yield row

    def initialize(self, data):
        super().initialize(data)
        data["query"] = {
            "method": self.query["method"],
            "targets": self.query["targets"],
            "molecule": json.dumps(self.qmol.jsonized()),
            "svg": SVG(self.qmol).contents()
        }

    def on_task_done(self, res, data):
        if res is not None:
            data["records"].append(res)
            self._update_founds(res["source"])
            data["dataSource"] = self.founds


class ExactStructFilter(SubStructFilter):
    def __init__(self, *args):
        super().__init__(*args)
        self.name = "Exact match"
        func = functools.partial(substructure.equal,
                                 ignore_hydrogen=self.query["ignoreHs"])
        self.row_processor = functools.partial(
            substr_processor, func, self.qmol)

    def args_generator(self):
        mw = molutil.mw_wo_sw(self.qmol)
        for conn, tbl in self._resources_iter():
            for res in conn.find_iter("_mw_wo_sw", (mw,), (tbl["table"],)):
                row = dict(res)
                row["source"] = tbl["id"]
                yield row


class SuperStructFilter(SubStructFilter):
    def __init__(self, *args):
        super().__init__(*args)
        self.name = "Superstructure"
        func = functools.partial(substructure.superstructure,
                                 ignore_hydrogen=self.query["ignoreHs"])
        self.row_processor = functools.partial(
            substr_processor, func, self.qmol)


def morgan_processor(qmol, thld, row, arg):
    mol = Compound(pickle.loads(arg[MOL.key]))
    try:
        score = rdkit.morgan_sim(mol, qmol, 4)
    except:
        print(traceback.format_exc())
        score = 0
    if score >= thld:
        row[MOL.key] = json.dumps(mol.jsonized())
        del arg[MOL.key]
        row.update(arg)
        row[SIM.key] = score
        return True
    return False


class RdMorganFilter(SubStructFilter):
    def __init__(self, *args):
        super().__init__(*args)
        self.name = "Morgan fingerprint"
        self.row_processor = functools.partial(
            morgan_processor, self.qmol, float(self.query["threshold"]))

    def initialize(self, data):
        super().initialize(data)
        data["query"].update({
            "threshold": self.query["threshold"],
            "thresholdType": self.query["thresholdType"]
        })
        data["extraColumns"] = [
            {"key": SIM.key, "name": SIM.name, "sort": SIM.sort}
        ]


def rdfmcs_processor(qmol, thldtype, thld, timeout, row, arg):
    mol = Compound(pickle.loads(arg[MOL.key]))
    type_ = {"sim": "similarity", "edge": "mcs_edges"}
    try:
        res = rdkit.fmcs(mol, qmol, timeout=timeout)
    except:
        print(traceback.format_exc())
        return False
    if res[type_[thldtype]] >= thld:
        row[MOL.key] = json.dumps(mol.jsonized())
        del arg[MOL.key]
        row.update(arg)
        row[SIM.key] = res["similarity"]
        row[MCSDR.key] = res["mcs_edges"]
        return True
    return False


class RdFmcsFilter(RdMorganFilter):
    def __init__(self, *args):
        super().__init__(*args)
        self.name = "RDKit FMCS"
        self.row_processor = functools.partial(
            rdfmcs_processor, self.qmol, self.query["thresholdType"],
            self.query["threshold"], self.query["timeout"])

    def initialize(self, data):
        super().initialize(data)
        data["query"].update({
            "timeout": self.query["timeout"],
        })
        data["extraColumns"].append(
            {"key": MCS.key, "name": MCS.name, "sort": MCS.sort}
        )


def mcsdr_processor(qmolarr, measure, thld, diam, tree, skip, row, arg):
    mol = Compound(pickle.loads(arg[MOL.key]))
    if len(mol) > skip:  # mol size filter
        return False
    try:
        arr = mcsdr.comparison_array(mol, diam, tree)
    except ValueError:
        return False
    sm, bg = sorted([arr[1], qmolarr[1]])
    if measure == "local_sim" and sm < bg * float(thld):  # threshold filter
        return False
    if measure == "mcsdr_edges" and sm < thld:  # fragment size filter
        return False
    res = mcsdr.local_sim(arr, qmolarr)
    if res[measure] >= thld:
        row[MOL.key] = json.dumps(mol.jsonized())
        del arg[MOL.key]
        row.update(arg)
        row[SIM.key] = res["local_sim"]
        row[MCSDR.key] = res["mcsdr_edges"]
        return True
    return False


class McsdrFilter(RdMorganFilter):
    def __init__(self, *args):
        super().__init__(*args)
        self.name = "MCS-DR"
        self.row_processor = functools.partial(
            mcsdr_processor, mcsdr.comparison_array(self.qmol), "mcsdr_edges",
            self.query["threshold"], self.query["diameter"],
            self.query["maxTreeSize"], self.query["molSizeCutoff"])

    def initialize(self, data):
        super().initialize(data)
        data["query"].update({
            "diameter": self.query["diameter"],
            "maxTreeSize": self.query["maxTreeSize"],
            "molSizeCutoff": self.query["molSizeCutoff"]
        })
        data["extraColumns"].append(
            {"key": MCSDR.key, "name": MCSDR.name, "sort": MCSDR.sort}
        )


class GlsFilter(McsdrFilter):
    def __init__(self, *args):
        super().__init__(*args)
        self.name = "Graph-based local similarity"
        self.row_processor = functools.partial(
            mcsdr_processor, mcsdr.comparison_array(self.qmol), "local_sim",
            self.query["threshold"], self.query["diameter"],
            self.query["maxTreeSize"], self.query["molSizeCutoff"])


def sdf_processor(cols, row, arg):
    for col in cols:
        row[col] = str(arg.data.get(col, ""))
    arg.data.clear()
    row[MOL.key] = json.dumps(arg.jsonized())
    return True


class SdfFilter(object):
    def __init__(self, file_, q):
        self.query = q
        self.file_content = file_
        self.row_processor = functools.partial(sdf_processor, q["columns"])

    def args_generator(self):
        """ should not use builder.queue """
        mols = v2000reader.mols_from_text(self.file_content)
        for m in mols:
            if self.query["implh"]:
                m = molutil.make_Hs_implicit(m)
            if self.query["recalc"]:
                calc2dcoords.calc2dcoords(m)
            yield m

    def initialize(self, data):
        data["format"] = "datatable"
        data["query"].update({
            "parsedOptions": self.query["columns"],
            "recalc2DCoords": self.query["recalc"],
            "makeHsImplicit": self.query["implh"]
        })
        for col in self.query["columns"]:
            data["columns"].append(
                {"key": col, "name": col, "sort": "text"}
            )


def matrix_processor(func, thld, row, arg):
    arr1, arr2 = arg
    result = func(arr1[1], arr2[1])
    if result >= thld:
        row["source"] = arr1[0]
        row["target"] = arr2[0]
        row["weight"] = result
        return True
    return False


class RdMorganMatrixFilter(object):
    def __init__(self, q):
        self.arrays = None
        self.arr_size = None
        self.query = q
        self._array_generator()
        self.row_processor = functools.partial(
            matrix_processor, rdkit.morgan_sim, self.query["threshold"])

    def _array_generator(self):
        molobjs = map(lambda x: Compound(json.loads(x)),
                      self.query["molecules"])
        mols = []
        for i, mol in zip(self.query["indices"], molobjs):
            if self.query["ignoreHs"]:
                mol = molutil.make_Hs_implicit(mol)
            mols.append((i, mol))
        self.arr_size = len(mols)
        self.arrays = mols

    def args_generator(self):
        return itertools.combinations(self.arrays, 2)

    def initialize(self, data):
        data["format"] = "connection"
        data["searchCount"] = int(self.arr_size * (self.arr_size - 1) / 2)
        data["nodeTableId"] = self.query["nodeTableId"]
        data["query"] = {
            "measure": self.query["measure"],
            "threshold": self.query["threshold"],
            "ignoreHs": self.query["ignoreHs"]
        }

    def on_task_done(self, res, data):
        if res is not None:
            data["records"].append(res)


def mcs_matrix_processor(func, param, thld, row, arg):
    arr1, arr2 = arg
    result = func(arr1[1], arr2[1])
    if result[param] >= thld:
        row["source"] = arr1[0]
        row["target"] = arr2[0]
        row["weight"] = result[param]
        if "canceled" in result:
            row["timeout"] = result["canceled"]
        return True
    return False


class RdFmcsMatrixFilter(RdMorganMatrixFilter):
    def __init__(self, q):
        self.arrays = None
        self.arr_size = None
        self.query = q
        self._array_generator()
        self.row_processor = functools.partial(
            mcs_matrix_processor,
            functools.partial(rdkit.fmcs, timeout=q["timeout"]),
            "similarity", float(q["threshold"])
        )

    def initialize(self, data):
        super().initialize(data)
        data["query"]["timeout"] = self.query["timeout"]


def gls_matrix_processor(func, param, thld, row, arg):
    sm, bg = sorted([arg[0][1][1], arg[1][1][1]])
    if sm < bg * thld:  # threshold filter
        return False
    return mcs_matrix_processor(func, param, thld, row, arg)


class GlsMatrixFilter(RdMorganMatrixFilter):
    def __init__(self, q):
        self.arrays = None
        self.arr_size = None
        self.query = q
        self._array_generator()
        self.row_processor = functools.partial(
            gls_matrix_processor, mcsdr.local_sim,
            "local_sim", q["threshold"])

    def _array_generator(self):
        mols = self.query["molecules"]
        idcs = self.query["indices"]
        ignoreHs = self.query["ignoreHs"]
        diam = self.query["diameter"]
        tree = self.query["maxTreeSize"]
        cutoff = self.query["molSizeCutoff"]
        molobjs = map(lambda x: Compound(json.loads(x)), mols)
        arrs = []
        for i, mol in zip(idcs, molobjs):
            if len(mol) > cutoff:
                continue
            try:
                arr = mcsdr.comparison_array(mol, diam, tree,
                                             ignore_hydrogen=ignoreHs)
                arrs.append((i, arr))
            except ValueError:
                pass
        self.arr_size = len(arrs)
        self.arrays = arrs

    def initialize(self, data):
        super().initialize(data)
        data["query"].update({
            "diameter": self.query["diameter"],
            "maxTreeSize": self.query["maxTreeSize"],
            "molSizeCutoff": self.query["molSizeCutoff"]
        })
