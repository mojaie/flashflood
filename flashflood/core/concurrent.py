#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from concurrent import futures as cf
import functools
import pickle
# import threading

from tornado import gen
from tornado.queues import Queue

from flashflood import functional
from flashflood import static
from flashflood.core.edge import AsyncEdge
from flashflood.core.node import Node
from flashflood.core.task import InvalidOperationError


class ConcurrentNode(Node):
    """Concurrent node

    ConcurrentNode is similar to FuncNode but do parallel computation by using
    given func and args. ConcurrentNode can have composite function passed from
    upstream FuncNodes. If ConcurrentNode has own function, this will be
    composed with upstream functions.

    ConcurrentNode has AsyncEdge as an outgoing edge. Parallel job results are
    sent to downstream using multiprocess queue. This does not guarantee same
    record order as the original one.

    ConcurrentNode can be used as a part of SubWorkflow

    Args:
        func(function): function to be applied
        sampler(core.container.Sampler): Sampler object

    Attributes:
        func(function): function to be applied
        queue(tornado.queues.Queue): multiprocess worker queue
    """
    def __init__(self, func=None, sampler=None, **kwargs):
        super().__init__(**kwargs)
        self.func = func
        self.queue = Queue(static.PROCESSES * 2)
        self._out_edge = AsyncEdge(sampler)
        self._interrupted = False
        self._cfunc = None
        self._cargs = None
        if self.func is not None:
            try:
                pickle.dumps(self.func)
            except (AttributeError, TypeError):
                raise InvalidOperationError(
                    "ConcurrentNode.func should be picklable")

    @gen.coroutine
    def run(self, on_finish, on_abort):
        if self.edge_type(self._in_edge) == "AsyncEdge":
            self.synchronizer()
        while 1:
            if self._in_edge.status == "aborted":
                on_abort()
                return
            if self._in_edge.status == "done":
                if self.edge_type(self._in_edge) == "FuncEdge":
                    if self.func is None:
                        self._cfunc = self._in_edge.func
                    else:
                        self._cfunc = functional.compose(
                            self.func, self._in_edge.func)
                else:
                    self._cfunc = self.func
                if self.edge_type(self._in_edge) != "AsyncEdge":
                    self._cargs = self._in_edge.records
                break
            yield gen.sleep(self.interval)
        with cf.ThreadPoolExecutor(static.PROCESSES) as tp:
            for p in range(static.PROCESSES):
                tp.submit(self.consumer())
            with cf.ProcessPoolExecutor(static.PROCESSES) as pp:
                for a in self._cargs:
                    yield self.queue.put(pp.submit(self._cfunc, a))
                    if self._interrupted:
                        yield self.queue.join()
                        yield self._out_edge.abort()
                        on_abort()
                        return
                yield self.queue.join()
            yield self._out_edge.done()
        on_finish()

    def interrupt(self):
        self._interrupted = True

    def on_submit(self):
        super().on_submit()
        if self.func is None and self.edge_type(self._in_edge) != "FuncEdge":
            raise InvalidOperationError("No concurrent function")

    @gen.coroutine
    def on_task_done(self, record):
        yield self._out_edge.put(record)

    @gen.coroutine
    def synchronizer(self):
        self._cargs = []
        while 1:
            in_ = yield self._in_edge.get()
            self._cargs.append(in_)

    @gen.coroutine
    def consumer(self):
        while True:
            f = yield self.queue.get()
            res = yield f
            # TODO: threading.Lock may be unnecessary
            # with threading.Lock():
            self.on_task_done(res)
            self.queue.task_done()


def filter_(pred, arg):
    if pred(arg):
        return arg


class ConcurrentFilter(ConcurrentNode):
    """Concurrent filter node

    ConcurrentFilter also does parallel computation by using given composed
    function and arguments(records) but it will pass only not None records to
    the downstream.

    Args:
        residue_counter(Counter): counter for records which are filtered out
        **kwargs: kwargs

    Attributes:
        residue_counter(Counter): counter for records which are filtered out
    """
    def __init__(self, pred, residue_counter=None, **kwargs):
        super().__init__(**kwargs)
        f = functools.partial(filter_, pred)
        if self.func is None:
            self.func = f
        else:
            self.func = functional.compose(f, self.func)
        self.residue_counter = residue_counter

    @gen.coroutine
    def on_task_done(self, record):
        if record is not None:
            yield self._out_edge.put(record)
        elif self.residue_counter is not None:
            self.residue_counter.value += 1
