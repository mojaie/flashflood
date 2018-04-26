#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from flashflood.node.control.filter import Filter, AsyncFilter


class StartsWith(Filter):
    def __init__(self, key, text, **kwargs):
        super().__init__(
            lambda x: x[key].startswith(text),
            residue_counter=None, fields=None, **kwargs)


class AsyncStartsWith(AsyncFilter):
    def __init__(self, key, text, **kwargs):
        super().__init__(
            lambda x: x[key].startswith(text),
            residue_counter=None, fields=None, **kwargs)
