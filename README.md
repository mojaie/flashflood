
Flashflood
================

Flashflood is a HTTP API server builder for chemical data analysis. Flashflood includes DAG workflow assistance for parallel processing of chemical structure, chemical properties, biochemical activity data. This enables to build web server which can be accessed by web application, Jupyter notebook and any other analysis platform via HTTP.



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

(C) 2014-2017 Seiji Matsuoka
