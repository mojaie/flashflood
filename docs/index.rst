
Flashflood Documentation
============================

Flashflood is a directed acyclic graph (DAG) workflow-based HTTP API server builder. Input data is processed according to the workflow definition (data processing nodes and dataflow edges) and then the result JSON (and other formats) will be broadcasted via HTTP. This offers instant database access API to users to share their results and analysis regardless of their programming langages and environments.


Features
----------

- HTTP API builder
- Directed acyclic graph (DAG) workflow to process database records
- Asynchronus workflow: you can get records processed so far from ongoing tasks
- Multiprocess (parallel) record processing
- Input from python list, SQLite table, text file (CSV-like) and SDFile.
- Chemical structure input (by using Chorus)
- Output to JSON, Excel, SDFile and SQLite


Contents:
-------------

.. toctree::
   :maxdepth: 1

   getting_started.rst
   build_workflow.rst
   api/api.rst


License
-------------

`MIT license <http://opensource.org/licenses/MIT>`_


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
