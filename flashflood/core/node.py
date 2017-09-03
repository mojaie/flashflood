#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood.core.edge import Edge, AsyncQueueEdge
from flashflood.core.task import Task


class Node(Task):
    """
    Parameters:
      status: str
        ready: ready to run
        done: finished and put all results to outgoing edges
    """
    def __init__(self):
        super().__init__()
        self.node_num = None
        self._in_edge = None

    def add_in_edge(self, edge, port):
        if port != 0:
            raise ValueError("invalid port")
        self._in_edge = edge

    def out_edge(self, port):
        if port != 0:
            raise ValueError("invalid port")
        return self._out_edge

    def on_submitted(self):
        self._out_edge.task_count = self._in_edge.task_count
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
    def __init__(self):
        super().__init__()
        self._out_edge = Edge()

    def run(self):
        self.on_finish()

    def on_submitted(self):
        super().on_submitted()
        # self._out_edge.records = self._in_edge.records


class AsyncNode(Node):
    def __init__(self):
        super().__init__()
        self._out_edge = AsyncQueueEdge()
        self.interval = 0.5

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            yield self._out_edge.put(in_)
            self._out_edge.done_count = self._in_edge.done_count

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


class LazyNode(AsyncNode):
    """For async node test"""
    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            yield gen.sleep(0.01)
            yield self._out_edge.put(in_)
            self._out_edge.done_count = self._in_edge.done_count


class Synchronizer(Node):
    def __init__(self):
        super().__init__()
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


class LazyConsumer(Synchronizer):
    """For async node test"""
    def __init__(self):
        super().__init__()
        self.task_count = 0
        self.done_count = 0
        self.records = []

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self._in_edge.get()
            self.records.append(in_)
            self.done_count = self._in_edge.done_count
            yield gen.sleep(0.01)

    def on_submitted(self):
        self.task_count = self._in_edge.task_count


class Asynchronizer(Node):
    def __init__(self):
        super().__init__()
        self._out_edge = AsyncQueueEdge()

    @gen.coroutine
    def run(self):
        self.on_start()
        for in_ in self._in_edge.records:
            if self.status == "interrupted":
                return
            yield self._out_edge.put(in_)
            self._out_edge.done_count += 1
        yield self._out_edge.done()
        self.on_finish()

    @gen.coroutine
    def interrupt(self):
        if self.status != "running":
            return
        self.status = "interrupted"
        yield self._out_edge.aborted()
        self.on_aborted()


class EagerProducer(Asynchronizer):
    def __init__(self):
        super().__init__()
        self._out_edge = AsyncQueueEdge()
        self.task_count = 1000

    @gen.coroutine
    def run(self):
        self.on_start()
        for i in range(self.task_count):
            if self.status == "interrupted":
                self.on_aborted()
                return
            yield self._out_edge.put(i)
            self._out_edge.done_count += 1
        yield self._out_edge.done()
        self.on_finish()
