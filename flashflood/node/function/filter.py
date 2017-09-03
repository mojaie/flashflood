#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from flashflood.core.task import MPWorker
from flashflood.core.node import SyncNode, Asynchronizer


class Filter(SyncNode):
    def __init__(self, func, fields=None, params=None):
        super().__init__()
        self.func = func
        if fields is not None:
            self.fields.merge(fields)
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        super().on_submitted()
        self._out_edge.records = filter(self.func, self._in_edge.records)


class MPNodeWorker(MPWorker):
    def __init__(self, args, func, node):
        super().__init__(args, func)
        self.node = node

    @gen.coroutine
    def on_task_done(self, record):
        self.node._out_edge.done_count += 1
        if record:
            yield self.node._out_edge.put(record)
        # TODO:
        # if not done_count % 100:
        #     yield self.node.out_edge.proceed()

    @gen.coroutine
    def on_finish(self):
        yield self.node._out_edge.done()
        self.node.on_finish()
        self.status = "done"

    @gen.coroutine
    def on_aborted(self):
        yield self.node._out_edge.aborted()
        self.node.on_aborted()
        self.status = "aborted"


class MPFilter(Asynchronizer):
    def __init__(self, func, fields=None, params=None):
        super().__init__()
        self.func = func
        self.worker = None
        if fields is not None:
            self.fields.merge(fields)
        if params is not None:
            self.params.update(params)

    @gen.coroutine
    def run(self):
        self.on_start()
        self.worker = MPNodeWorker(self._in_edge.records, self.func, self)
        yield self.worker.run()

    @gen.coroutine
    def interrupt(self):
        if self.status != "running":
            return
        yield self.worker.interrupt()
