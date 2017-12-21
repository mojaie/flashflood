
Building workflow
====================================================================


.. code-block:: python

    from tornado.ioloop import IOLoop

    from flashflood.core.node import FuncNode
    from flashflood.core.workflow import Workflow
    from flashflood.node.reader.iterinput import IterInput


    class TestWorkflow(Workflow):
        def __init__(self):
            super().__init__()
            self.output = Container()
            self.append(IterInput(range(100)))
            self.append(FuncNode())
            self.append(ContainerWriter(output))


    if __name__ == '__main__':
        wf = TestWorkflow()
        task = Task(wf)
        IOLoop.current().run_sync(task.execute())
        print(wf.output.records)


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
