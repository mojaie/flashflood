#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood.util import graph
from flashflood.core.node import Asynchronizer
from flashflood.core.task import Task


class Workflow(Task):
    def __init__(self, verbose=True):
        super().__init__()
        self.nodes = []
        self.preds = {}
        self.succs = {}
        self.order = None
        self.interval = 0.5
        self.datatype = "nodes"
        # TODO: verbose output
        self.verbose = verbose

    def on_submitted(self):
        # TODO: catch exceptions on submit (ex. file path error, format error)
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

    @gen.coroutine
    def run(self):
        self.on_start()
        for node_id in self.order:
            self.nodes[node_id].run()
        while any(n.status == "running" for n in self.nodes):
            if self.status == "aborted":
                return
            yield gen.sleep(self.interval)
        self.on_finish()

    @gen.coroutine
    def interrupt(self):
        self.status = "interrupted"
        # TODO: Only an Asynchronizer might be running which will be stopped
        # TODO: sqlitewriter should allow interruption
        fs = [
            n.interrupt() for n in self.nodes if isinstance(n, Asynchronizer)]
        while not all(f.done() for f in fs):
            yield gen.sleep(self.interval)
        self.on_aborted()

    @gen.coroutine
    def submit(self):
        self.on_submitted()
        yield self.run()

    def connect(self, up, down, up_port=0, down_port=0):
        """Adds a workflow connection
            Args:
                up: source node
                down: target node
                port: output port of source node
        """
        if up.node_num is None:
            up.node_num = len(self.nodes)
            self.nodes.append(up)
            self.preds[up.node_num] = {}
            self.succs[up.node_num] = {}
        if down.node_num is None:
            down.node_num = len(self.nodes)
            self.nodes.append(down)
            self.preds[down.node_num] = {}
            self.succs[down.node_num] = {}
        self.preds[down.node_num][up.node_num] = up_port
        self.succs[up.node_num][down.node_num] = down_port


class SubWorkflow(Workflow):
    def __init__(self, params=None, verbose=False):
        super().__init__(verbose=verbose)
        self.node_num = None
        self._in_edge = None
        self._out_edge = None
        self._entrance = None
        self._entrance_port = None
        self._exit = None
        self._exit_port = None
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        self.nodes[self._entrance].add_in_edge(
            self._in_edge, self._entrance_port)
        super().on_submitted()
        self._out_edge = self.nodes[self._exit].out_edge(self._exit_port)
        self._out_edge.fields.merge(self._in_edge.fields)
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self._in_edge.params)
        self._out_edge.params.update(self.params)

    def add_in_edge(self, edge, port):
        if port != 0:
            raise ValueError("invalid port")
        self._in_edge = edge

    def out_edge(self, port):
        if port != 0:
            raise ValueError("invalid port")
        return self._out_edge

    def set_entrance(self, node, port=0):
        if node.node_num is None:
            node.node_num = len(self.nodes)
            self.nodes.append(node)
        self._entrance = node.node_num
        self._entrance_port = port

    def set_exit(self, node, port=0):
        if node.node_num is None:
            node.node_num = len(self.nodes)
            self.nodes.append(node)
        self._exit = node.node_num
        self._exit_port = port
