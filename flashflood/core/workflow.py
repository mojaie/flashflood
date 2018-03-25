#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from flashflood import graph
from flashflood.core.task import Task, TaskSpecs, InvalidOperationError


class Workflow(TaskSpecs):
    """Flashflood workflow class

    Attributes:
        nodes (list): list of node numbers
            (`flashflood.core.node.Node.node_num`)
        tasks (list): list of tasks in order of execution
        preds (dict): workflow graph connection (predecessors)
        succs (dict): workflow graph connection (successors)
        interval (float): worker thread loop interval time (in second)
        verbose (bool): if verbose output is generated or not
    """
    def __init__(self):
        self.nodes = []
        self.tasks = []
        self.preds = {}
        self.succs = {}
        self.interval = 0.5
        self.verbose = False
        self._interrupted = False
        self._aborting = False

    @gen.coroutine
    def run(self, on_finish, on_abort):
        for task in self.tasks:
            task.run()
        while 1:
            if self._interrupted and not self._aborting:
                for n in self.nodes:
                    n.interrupt()
                self._aborting = True
            if all(t.status != "running" for t in self.tasks):
                if self._aborting:
                    on_abort()
                else:
                    on_finish()
                return
            yield gen.sleep(self.interval)

    def on_submit(self):
        self.build_workflow()

    def on_start(self):
        pass

    def on_finish(self):
        pass

    def interrupt(self):
        self._interrupted = True

    def on_abort(self):
        pass

    def connect(self, up, down, up_port=0, down_port=0):
        """Adds a workflow connection

        Args:
            up (flashflood.core.node.Node): source node
            down (flashflood.core.node.Node): target node
            up_port (int): output port of the source node
            down_port (int): input port of the target node
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

    def append(self, node, up_port=0, down_port=0):
        """Adds a node connected to the latest added node

        Args:
            node (flashflood.core.node.Node): target node
            up_port (int): output port of the source node
            down_port (int): input port of the target node
        """
        if node.node_num is not None:
            raise InvalidOperationError(
                "This node is already assigned to the workflow")
        node.node_num = len(self.nodes)
        self.nodes.append(node)
        self.preds[node.node_num] = {}
        self.succs[node.node_num] = {}
        latest = self.nodes[-1].node_num - 1
        if latest != -1:
            self.preds[node.node_num][latest] = up_port
            self.succs[latest][node.node_num] = down_port

    def build_workflow(self):
        """Build workflow graph connection and determine order of execution.
        """
        # TODO: catch exceptions on submit (ex. file path error, format error)
        if not self.nodes:
            raise InvalidOperationError("No nodes are registered")
        if len(self.nodes) == 1:
            self.nodes[0].interval = self.interval
            task = Task(self.nodes[0], verbose=self.verbose)
            self.tasks.append(task)
            task.on_submit()
        else:
            order = graph.topological_sort(self.succs, self.preds)
            for up in order:
                self.nodes[up].interval = self.interval
                task = Task(self.nodes[up], verbose=self.verbose)
                self.tasks.append(task)
                task.on_submit()
                for down, dport in self.succs[up].items():
                    uport = self.preds[down][up]
                    self.nodes[down].add_in_edge(
                        self.nodes[up].out_edge(uport), dport)


class SubWorkflow(Workflow):
    """Subworkflow class

    Args:
        node_ext (flashflood.core.node.Node): node object used for external
            connection
        **kwargs: kwargs
    """
    def __init__(self, node_ext, **kwargs):
        super().__init__(**kwargs)
        self.node_ext = node_ext
        self.node_num = None
        self._entrance = None
        self._entrance_port = None
        self._exit = None
        self._exit_port = None

    @gen.coroutine
    def run(self, on_finish, on_abort):
        super().run(lambda: None, on_abort)
        self.node_ext.run(on_finish, on_abort)

    def on_submit(self):
        self.build_workflow()
        self.node_ext._in_edge = self.nodes[self._exit].out_edge(
            self._exit_port)
        self.node_ext.merge_fields()
        self.node_ext.update_params()

    def interrupt(self):
        for n in self.nodes:
            n.interrupt()
        self.node_ext.interrupt()

    def add_in_edge(self, edge, port):
        if port != 0:
            raise ValueError("invalid port")
        self.nodes[self._entrance].add_in_edge(edge, port)

    def out_edge(self, port):
        if port != 0:
            raise ValueError("invalid port")
        return self.node_ext._out_edge

    def edge_type(self, edge):
        return self.node_ext.edge_type(edge)

    def set_entrance(self, node, port=0):
        """Set node which connect to the upstream of the subworkflow

        Args:
            node (int): entrance node number
            port (int): port which connect to the incoming edge
        """
        if node.node_num is None:
            node.node_num = len(self.nodes)
            self.nodes.append(node)
        self._entrance = node.node_num
        self._entrance_port = port

    def set_exit(self, node, port=0):
        """Set node which connect to the downstream of the subworkflow

        Args:
            node (int): exit node number
            port (int): port which connect to the outgoing edge
        """
        if node.node_num is None:
            node.node_num = len(self.nodes)
            self.nodes.append(node)
        self._exit = node.node_num
        self._exit_port = port
