
Flashflood
================

Flashflood is a HTTP API server builder for chemical data analysis. Flashflood includes DAG workflow assistance for parallel processing of chemical structure, chemical properties, biochemical activity data. This enables to build web server which can be accessed by web application, Jupyter notebook and any other analysis platform via HTTP.


Example
================

Workflow samples are available at [flashflood-workspace-sample](https://github.com/mojaie/flashflood-workspace-sample)

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

see [flashflood-workspace-sample](https://github.com/mojaie/flashflood-workspace-sample)



Documentation
-------------------

https://mojaie.github.io/flashflood



License
--------------

[MIT license](http://opensource.org/licenses/MIT)



Copyright
--------------

(C) 2014-2018 Seiji Matsuoka
