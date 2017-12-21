#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#
"""List of dict (LOD) module"""


class ListOfDict(list):
    """List of dict utility class
    """
    def values(self, key):
        """Returns record values with the given key"""
        return map(lambda x: x[key], self)

    def filter(self, key, value):
        """Filters by the given key-value pair"""
        return filter(lambda x: x[key] == value, self)

    def find(self, key, value):
        """Returns the record found by the given key-value pair.
        if not found, return None
        """
        return next(self.filter(key, value), None)

    def add(self, rcd, key="key", dupkey="replace"):
        """Adds a new record.

        Duplicate key (dupkey) operations

        * replace - replace existing record with the new one (default)
        * update - update exisiting record (dict.update)
        * skip - add no record when the key is duplicated
        * replace or update will be applied for only the first one found

        Args:
            rcd (dict): New record to be added
            key (str): Record key
            dupkey (str): Type of duplicate key operation
        """
        for i, r in enumerate(self):
            if r[key] == rcd[key]:
                if dupkey == "skip":
                    return
                if dupkey == "update":
                    self[i].update(rcd)
                    return
                if dupkey == "replace":
                    self[i] = rcd
                    return
        self.append(rcd)

    def reduce(self, key="key", dupkey="update"):
        """Removes records with duplicated key

        Not appropriate for large data - worst case: O(n^2)

        Duplicate key (dupkey) operations

        * update - update exisiting record (dict.update)
        * skip - add no record when the key is duplicated

        Args:
            key (str): Record key
            dupkey (str): Type of duplicate key operation
        """
        new = ListOfDict()
        for r in self:
            new.add(r, key=key, dupkey=dupkey)
        self.clear()
        self.extend(new)

    def unique(self, key="key"):
        """Aliase of reduce(dupkey="skip")"""
        return self.reduce(key, dupkey="skip")

    def merge(self, rcds, key="key", dupkey="replace"):
        """Adds list of records

        Args:
            rcds (list): List of dict records to be added
            key (str): Record key
            dupkey (str): Type of duplicate key operation
                (see `ListOfDict.add`)
        """
        for r in rcds:
            self.add(r, key=key, dupkey=dupkey)

    def join(self, rcds, key, full_join=False):
        """Left join records

        Args:
            rcds (list): List of dict records to be joined
            key (str): Join key
            full_join (bool): If true, do full join (join all records
                regardless of whether key exists)
        """
        idx = {}
        for L in self:
            idx[L[key]] = L
        for r in rcds:
            if r[key] in idx:
                idx[r[key]].update(r)
            elif full_join:
                self.append(r)

    def delete(self, key, value):
        """Removes records which matches the key-value pair

        Args:
            key (str): key
            value: value
        """
        new = ListOfDict(filter(lambda x: x[key] != value, self))
        self.clear()
        self.extend(new)

    def pick(self, key, value):
        """Removes a record which matches the key-value pair from the list
        and return the record.

        If no records are found, this returns None.

        Args:
            key (str): key
            value: value
        """
        for i, e in enumerate(self):
            if e[key] == value:
                return self.pop(i)


LOD = ListOfDict
"""Shorthand of ListOfDict
"""


def valuelist(lod, key):
    """Returns list of values which are assigned to the key.

    Args:
        lod: `flashflood.lod.ListOfDict`
        key: key

    Returns:
        list: list of values
    """
    return list(map(lambda x: x[key], lod))


def filtered(lod, key, value):
    """Returns filtered records by the key-value pair.

    Args:
        key: key
        value: value

    Returns:
        flashflood.lod.ListOfDict: filtered records
    """
    return ListOfDict(filter(lambda x: x[key] == value, lod))
