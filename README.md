
kiwiii-server
================

Kiwiii is a web-based client-server application for chemical data analysis and visualization. The server implements web API which enables easy access to databases via HTTP.



Installation
--------------

### PyPI

```
pip3 install kiwiii-server
```


### Anaconda

```
conda upgrade -n root conda
conda install -n root conda-build

conda skeleton pypi kiwiii-server
conda build kiwiii-server
conda install --use-local kiwiii-server
```



API Documentation
-------------------

https://mojaie.github.io/kiwiii-server



License
--------------

[MIT license](http://opensource.org/licenses/MIT)



Copyright
--------------

(C) 2014-2017 Seiji Matsuoka
