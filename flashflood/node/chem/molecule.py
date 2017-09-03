#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import json
import pickle

from chorus.model.graphmol import Compound

from flashflood import static
from flashflood.node.function.apply import Apply, AsyncApply


def chem_data(chem_calcs, pickle_mol, row):
    new_row = {}
    mol = Compound(json.loads(row["_molobj"]))
    new_row.update(row)
    new_row.update({k: v(mol) for k, v in chem_calcs.items()})
    if pickle_mol:
        new_row["_molobj"] = pickle.dumps(
            json.loads(row["_molobj"]), protocol=4)
    return new_row


class Molecule(Apply):
    def __init__(self, chem_calcs=None, pickle_mol=False,
                 fields=None, params=None):
        super().__init__(None, fields=fields, params=params)
        if fields is None:
            self.fields.merge(static.CHEM_FIELDS)
        if chem_calcs is None:
            chem_calcs = static.CHEM_FUNCTIONS
        self.func = functools.partial(chem_data, chem_calcs, pickle_mol)


class AsyncMolecule(AsyncApply):
    def __init__(self, chem_calcs=None, pickle_mol=False,
                 fields=None, params=None):
        super().__init__(None, fields=fields, params=params)
        if fields is None:
            self.fields.merge(static.CHEM_FIELDS)
        if chem_calcs is None:
            chem_calcs = static.CHEM_FUNCTIONS
        self.func = functools.partial(chem_data, chem_calcs, pickle_mol)
