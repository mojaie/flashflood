
Flashflood Documentation
============================

Flashflood is a HTTP API server builder for chemical data analysis. Flashflood includes DAG workflow assistance for parallel processing of chemical structure, chemical properties, biochemical activity data. This enables to build web server which can be accessed by web application, Jupyter notebook and any other analysis platform via HTTP.


Features
----------

- RESTful HTTP API builder
- DAG workflow for analysis and database construction
- Parallel computation
- Chemical database construction with SQLite3
- Library search (text match, chemical properties, sub/super structure)
- Chemical space network generation (MCS-DR, RDKit fingerprint and RDKit MCS)
- Output and reporting (JSON, Excel, SDFile and SQLite)


Contents:
-------------

.. toctree::
   :maxdepth: 1

   getting_started.rst
   build_workflow.rst
   http/http.rst
   api/api.rst


License
-------------

`MIT license <http://opensource.org/licenses/MIT>`_

Test datasets provided by `DrugBank <https://www.drugbank.ca/>`_  are permitted to use under `Creative Common’s by-nc 4.0 License <https://creativecommons.org/licenses/by-nc/4.0/legalcode>`_


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
