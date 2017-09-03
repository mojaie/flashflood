#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood.core.node import Node, Synchronizer


class JSONResponse(Node):
    def __init__(self, wf):
        super().__init__()
        self.wf = wf

    def run(self):
        self.wf.result_records = list(self._in_edge.records)
        self.wf.result_count = len(self.wf.result_records)
        self.wf.done_count = self._in_edge.task_count
        self.on_finish()

    def on_submitted(self):
        # Re-order fields
        self.fields.merge(self._in_edge.fields)
        self.fields.delete("key", "_molobj")
        idx = self.fields.pick("key", "_index")
        if idx:
            self.wf.fields.add(idx)
        struct = self.fields.pick("key", "_structure")
        if struct:
            self.wf.fields.add(struct)
        self.wf.fields.merge(self.fields)

        self.wf.done_count = 0
        self.wf.task_count = self._in_edge.task_count


class AsyncJSONResponse(Synchronizer):
    def __init__(self, wf):
        super().__init__()
        self.wf = wf

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            self.wf.result_records.append(in_)
            self.wf.result_count += 1
            self.wf.done_count = self._in_edge.done_count

    def on_submitted(self):
        # Re-order fields
        self.fields.merge(self._in_edge.fields)
        self.fields.delete("key", "_molobj")
        idx = self.fields.pick("key", "_index")
        if idx:
            self.wf.fields.add(idx)
        struct = self.fields.pick("key", "_structure")
        if struct:
            self.wf.fields.add(struct)
        self.wf.fields.merge(self.fields)

        self.wf.result_records = []
        self.wf.result_count = 0
        self.wf.task_count = self._in_edge.task_count
        self.wf.done_count = 0
