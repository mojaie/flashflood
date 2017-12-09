#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from concurrent import futures as cf
# import threading

from tornado import gen
from tornado.queues import Queue

from flashflood import static
from flashflood.core.edge import AsyncQueueEdge
from flashflood.core.workflow import SubWorkflow
from flashflood.util import graph


class ConcurrentSubWorkflow(SubWorkflow):
    """General-purpose multiprocess subworkflow"""
    def __init__(self, params=None, verbose=False):
        super().__init__(params=params, verbose=verbose)
        self._out_edge = AsyncQueueEdge()
        self._queue = Queue(static.PROCESSES * 2)
        self.interval = 0.5

    def on_submitted(self):
        self.nodes[self._entrance].add_in_edge(
            self._in_edge, self._entrance_port)
        if not self.nodes:
            raise ValueError("No nodes are registered")
        elif len(self.nodes) == 1:
            self.order = [self.nodes[0].node_num]
            self.nodes[0].on_submitted()
        else:
            self.order = graph.topological_sort(self.succs, self.preds)
            for up in self.order:
                self.nodes[up].on_submitted()
                for down, dport in self.succs[up].items():
                    uport = self.preds[down][up]
                    self.nodes[down].add_in_edge(
                        self.nodes[up].out_edge(uport), dport)
        self._out_edge.fields.merge(self._in_edge.fields)
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self._in_edge.params)
        self._out_edge.params.update(self.params)

    @gen.coroutine
    def run(self):
        self.on_start()
        for node_id in self.order:
            self.nodes[node_id].run()
        while any(n.status == "running" for n in self.nodes):
            if self.status == "aborted":
                return
            yield gen.sleep(self.interval)
        args = self.nodes[self._exit].out_edge(0).records
        func = self.nodes[self._exit].func
        with cf.ThreadPoolExecutor(static.PROCESSES) as tp:
            for p in range(static.PROCESSES):
                tp.submit(self._consumer())
            with cf.ProcessPoolExecutor(static.PROCESSES) as pp:
                for i, a in enumerate(args):
                    yield self._queue.put(pp.submit(func, a))
                    if self.status == "interrupted":
                        yield self._queue.join()
                        self.on_aborted()
                        return
                yield self._queue.join()
        self.on_finish()

    @gen.coroutine
    def on_task_done(self, record):
        yield self._out_edge.put(record)

    @gen.coroutine
    def on_finish(self):
        yield self._out_edge.done()
        self.status = "done"

    @gen.coroutine
    def on_aborted(self):
        yield self._out_edge.aborted()
        self.status = "aborted"

    @gen.coroutine
    def _consumer(self):
        while True:
            f = yield self._queue.get()
            res = yield f
            # TODO: threading.Lock may be unnecessary
            # with threading.Lock():
            self.on_task_done(res)
            self._queue.task_done()

    @gen.coroutine
    def interrupt(self):
        self.status = "interrupted"
        while self.status != "aborted":
            yield gen.sleep(self.interval)


class ConcurrentFilterSubWorkflow(ConcurrentSubWorkflow):
    def __init__(self, residue_counter=None, params=None, verbose=False):
        super().__init__(params=params, verbose=verbose)
        self.residue_counter = residue_counter

    @gen.coroutine
    def on_task_done(self, record):
        if record:
            yield self._out_edge.put(record)
        elif self.residue_counter is not None:
            self.residue_counter += 1
