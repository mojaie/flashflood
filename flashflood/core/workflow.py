#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time

from tornado import gen

from flashflood import static
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

    @gen.coroutine
    def submit(self):
        self.on_submitted()
        yield self.run()

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

    def on_submitted(self):
        # TODO: catch exceptions on submit (ex. file path error, format error)
        self.order = graph.topological_sort(self.succs, self.preds)
        for up in self.order:
            for down, dport in self.succs[up].items():
                uport = self.preds[down][up]
                self.nodes[down].add_in_edge(
                    self.nodes[up].out_edge(uport), dport)
            self.nodes[up].on_submitted()
