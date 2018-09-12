
Flashflood
================

Flashflood is a directed acyclic graph (DAG) workflow-based HTTP API server builder. Input data is processed according to the workflow definition (data processing nodes and dataflow edges) and then the result JSON (and other formats) will be broadcasted via HTTP. This offers instant database access API to users to share their results and analysis regardless of their programming langages and environments.


Example
================

Workflow samples are available at [flashflood-workspace](https://github.com/mojaie/flashflood-workspace)

```py
    from tornado.ioloop import IOLoop

    from flashflood.core.node import FuncNode
    from flashflood.core.workflow import Workflow
    from flashflood.node.reader.iterinput import IterInput


    def twice(x):
        return x * 2


    class TestWorkflow(Workflow):
        def __init__(self):
            super().__init__()
            self.output = Container()
            self.append(IterInput(range(100)))
            self.append(FuncNode(twice))
            self.append(ContainerWriter(output))


    if __name__ == '__main__':
        wf = TestWorkflow()
        task = Task(wf)
        IOLoop.current().run_sync(task.execute)
        print(wf.output.records)
```


Installation
--------------

### PyPI

```
pip3 install flashflood
```


### Anaconda

```
conda upgrade -n root conda
conda install -n root conda-build

conda skeleton pypi flashflood
conda build flashflood
conda install --use-local flashflood
```


Building workspace
---------------------

see [flashflood-workspace](https://github.com/mojaie/flashflood-workspace)



Documentation
-------------------

https://flashflood-docs.readthedocs.io/



License
--------------

[MIT license](http://opensource.org/licenses/MIT)



Copyright
--------------

(C) 2014-2018 Seiji Matsuoka
