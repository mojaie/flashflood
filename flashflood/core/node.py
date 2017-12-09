#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood import functional
from flashflood.core.edge import Edge, FunctionEdge, AsyncQueueEdge
from flashflood.core.task import Task


class Node(Task):
    """
    Parameters:
      status: str
        ready: ready to run
        done: finished and put all results to outgoing edges
    """
    def __init__(self, params=None):
        super().__init__()
        self.node_num = None
        self._in_edge = None
        if params is not None:
            self.params.update(params)

    def add_in_edge(self, edge, port):
        if port != 0:
            raise ValueError("invalid port")
        self._in_edge = edge

    def out_edge(self, port):
        if port != 0:
            raise ValueError("invalid port")
        return self._out_edge

    def on_submitted(self):
        self._out_edge.fields.merge(self._in_edge.fields)
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self._in_edge.params)
        self._out_edge.params.update(self.params)


class SyncNode(Node):
    """
    Parameters:
      status: str
        ready: ready to run
        done: finished and put all results to outgoing edges
    """
    def __init__(self, params=None):
        super().__init__(params=params)
        self._out_edge = Edge()

    def on_submitted(self):
        super().on_submitted()
        if isinstance(self._in_edge, Edge):
            pass
            # self._out_edge.records = self._in_edge.records
        elif isinstance(self._in_edge, FunctionEdge):
            self._out_edge.records = map(
                self._in_edge.func, self._in_edge.records)
        else:
            raise ValueError("Invalid upstream edge.")


class FunctionNode(Node):
    """
    Parameters:
      status: str
        ready: ready to run
        done: finished and put all results to outgoing edges
    """
    def __init__(self, func, params=None):
        super().__init__(params=params)
        self.func = func
        self._out_edge = FunctionEdge()

    def on_submitted(self):
        super().on_submitted()
        if isinstance(self._in_edge, Edge):
            self._out_edge.func = self.func
            self._out_edge.records = self._in_edge.records
        elif isinstance(self._in_edge, FunctionEdge):
            self._out_edge.func = functional.compose(
                self.func, self._in_edge.func)
            self._out_edge.records = self._in_edge.records
        else:
            raise ValueError("Invalid upstream edge.")


class AsyncNode(Node):
    def __init__(self, params=None):
        super().__init__(params=params)
        self._out_edge = AsyncQueueEdge()
        self.interval = 0.5

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            yield self._out_edge.put(in_)

    @gen.coroutine
    def run(self):
        self.on_start()
        self._get_loop()
        while 1:
            if self._in_edge.status == "aborted":
                self._out_edge.status = "aborted"
                self.on_aborted()
                break
            if self._in_edge.status == "done":
                self._out_edge.status = "done"
                self.on_finish()
                break
            yield gen.sleep(self.interval)


class Synchronizer(Node):
    def __init__(self, params=None):
        super().__init__(params=params)
        self._out_edge = Edge()
        self.interval = 0.5

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            self._out_edge.records.append(in_)

    @gen.coroutine
    def run(self):
        self.on_start()
        self._get_loop()
        while 1:
            if self._in_edge.status == "aborted":
                self._out_edge.status = "aborted"
                self.on_aborted()
                break
            if self._in_edge.status == "done":
                self._out_edge.status = "done"
                self.on_finish()
                break
            yield gen.sleep(self.interval)


class Asynchronizer(Node):
    def __init__(self, params=None):
        super().__init__(params=params)
        self._out_edge = AsyncQueueEdge()

    @gen.coroutine
    def run(self):
        self.on_start()
        for in_ in self._in_edge.records:
            if self.status == "interrupted":
                return
            yield self._out_edge.put(in_)
        yield self._out_edge.done()
        self.on_finish()

    @gen.coroutine
    def interrupt(self):
        if self.status != "running":
            return
        self.status = "interrupted"
        yield self._out_edge.aborted()
        self.on_aborted()
