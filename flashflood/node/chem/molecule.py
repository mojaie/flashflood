#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import pickle

from chorus import molutil
from chorus.model.graphmol import Compound

from flashflood import static
from flashflood.node.field.extend import Extend, AsyncExtend


def jsonize(mol):
    return json.dumps(mol.jsonized())


def mol_from_json(j):
    return Compound(json.loads(j))


def pickle_mol(mol):
    return pickle.dumps(mol.jsonized(), protocol=4)


def unpickle_mol(p):
    return Compound(pickle.loads(p))


class MoleculeToJSON(Extend):
    def __init__(self, in_place=True, **kwargs):
        super().__init__("__moljson", "__molobj", func=jsonize,
                         in_place=in_place,
                         fill=json.dumps(molutil.null_molecule().jsonized()),
                         fields=[static.MOLJSON_FIELD], **kwargs)


class AsyncMoleculeToJSON(AsyncExtend):
    def __init__(self, in_place=True, **kwargs):
        super().__init__("__moljson", "__molobj", func=jsonize,
                         in_place=in_place,
                         fill=json.dumps(molutil.null_molecule().jsonized()),
                         fields=[static.MOLJSON_FIELD], **kwargs)


class MoleculeFromJSON(Extend):
    def __init__(self, in_place=True, **kwargs):
        super().__init__("__molobj", "__moljson", func=mol_from_json,
                         in_place=in_place, fill=molutil.null_molecule(),
                         fields=[static.MOLOBJ_FIELD], **kwargs)


class PickleMolecule(Extend):
    def __init__(self, in_place=True, **kwargs):
        super().__init__(
            "__molpickle", "__molobj", func=pickle_mol, in_place=in_place,
            fill=pickle.dumps(molutil.null_molecule().jsonized(), protocol=4),
            fields=[static.MOLPICKLE_FIELD], **kwargs)


class UnpickleMolecule(Extend):
    def __init__(self, in_place=True, **kwargs):
        super().__init__("__molobj", "__molpickle", func=unpickle_mol,
                         in_place=in_place, fill=molutil.null_molecule(),
                         fields=[static.MOLOBJ_FIELD], **kwargs)
