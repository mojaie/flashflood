#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools
import json
import os
import yaml

from chorus import molutil, wclogp
from chorus.draw.svg import SVG
from chorus.model.graphmol import Compound

from kiwiii.util import lod

with open(os.path.join(
        os.path.dirname(__file__), "../server_config.yaml")) as f:
    config = yaml.load(f.read())


class MolObjectColumn(object):
    def __init__(self):
        self.key = "_mol"


class SimilarityColumn(object):
    def __init__(self):
        self.key = "_similarity"
        self.name = "Similarity"
        self.sort = "numeric"


class McsSizeColumn(object):
    def __init__(self):
        self.key = "_mcssize"
        self.name = "MCS edge count"
        self.sort = "numeric"


class McsdrSizeColumn(object):
    def __init__(self):
        self.key = "_mcsdrsize"
        self.name = "MCS-DR edge count"
        self.sort = "numeric"


def row_processor(key, row, arg):
    """(row, arg) => return pred function
    Avoid using lambda, generator and nested func.
    row_processor should be picklable for multiprocess computation.
    """
    row[key] = arg
    return True


class IndexColumn(object):
    def __init__(self):
        self.key = "_index"
        self.name = "Index"
        self.sort = "numeric"
        self.row_processor = functools.partial(row_processor, self.key)

    def args_generator(self):
        """Returns iterator of args for row_processor"""
        return itertools.count(1)

    def initialize(self, data):
        # index column is primary
        data["columns"].insert(
            0, {"key": self.key, "name": self.name, "sort": self.sort}
        )


def chem_processor(func, key, row, _):
    molobj = MolObjectColumn().key
    mol = Compound(json.loads(row[molobj]))
    row[key] = func(mol)
    return True


def mol_to_svg(mol):
    return SVG(mol).contents()


class StructureColumn(object):
    def __init__(self):
        super().__init__()
        self.key = "_structure"
        self.name = "Structure"
        self.sort = "none"
        self.function = mol_to_svg
        self.row_processor = functools.partial(
            chem_processor, self.function, self.key)

    def args_generator(self):
        return itertools.repeat(None)  # dummy

    def initialize(self, data):
        # structure column is next to the index
        data["columns"].insert(
            1, {"key": self.key, "name": self.name, "sort": self.sort}
        )


class MwColumn(StructureColumn):
    def __init__(self):
        self.key = "_mw"
        self.name = "MW"
        self.sort = "numeric"
        self.function = molutil.mw
        self.row_processor = functools.partial(
            chem_processor, self.function, self.key)


class MwwoswColumn(MwColumn):
    def __init__(self):
        self.key = "_mw_wo_sw"
        self.name = "MW w/o salt and water"
        self.sort = "numeric"
        self.function = molutil.mw_wo_sw
        self.row_processor = functools.partial(
            chem_processor, self.function, self.key)


class FormulaColumn(MwColumn):
    def __init__(self):
        self.key = "_formula"
        self.name = "Formula"
        self.sort = "text"
        self.function = molutil.formula
        self.row_processor = functools.partial(
            chem_processor, self.function, self.key)


class WclogpColumn(MwColumn):
    def __init__(self):
        self.key = "_logp"
        self.name = "Wildman-Crippen LogP"
        self.sort = "numeric"
        self.function = wclogp.wclogp
        self.row_processor = functools.partial(
            chem_processor, self.function, self.key)


class NonHydrogenColumn(MwColumn):
    def __init__(self):
        self.key = "_nonH"
        self.name = "Non-hydrogen atom count"
        self.sort = "numeric"
        self.function = molutil.non_hydrogen_count
        self.row_processor = functools.partial(
            chem_processor, self.function, self.key)


CALC_COLS = [
    MwColumn(),
    MwwoswColumn(),
    FormulaColumn(),
    WclogpColumn(),
    NonHydrogenColumn()
]


def add_calc_cols(table):
    # columns to be calculated
    for col in CALC_COLS:
        if lod.find("key", col.key, table["columns"]) is not None:
            continue  # Skip pre-calculated
        table["columns"].append({
            "key": col.key,
            "name": col.name,
            "sort": col.sort,
            "method": "chemcalc"
        })


def calcgroup_processor(row, _):
    molobj = MolObjectColumn().key
    mol = Compound(json.loads(row[molobj]))
    for col in CALC_COLS:
        if col.key in row:
            continue  # skip pre-calculated
        row[col.key] = col.function(mol)
    return True


class CalcColumnGroup(object):
    def __init__(self):
        self.row_processor = calcgroup_processor

    def args_generator(self):
        return itertools.repeat(None)  # dummy
