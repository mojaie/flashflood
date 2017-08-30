
Query API
=======================


.. autotornado:: kiwiii.handler:app
    :endpoints:


Response JSON format
-----------------------


**Datatable response**::

    {
        "id": "hoge",
        "format": "datatable",
        "progress": 100
    }


:Attributes:
    * **id**\ (*string*) - datatable ID
    * **format**\ (*string*) - "datatable"
    * **startDate**\ (*string*) - date computation job started
    * **responseDate**\ (*string*) - date of response sent from server
    * **execTime**\ (*float*) - execution time
    * **searchCount**\ (*int*) - sum of the number of rows to search
    * **searchDoneCount**\ (*int*) - sum of the number of rows searched
    * **progress**\ (*float*) - 100
    * **recordCount**\ (*int*) - number of rows found
    * **dataSource**\ (*string*) - list of database entity
    * **query**\ (*object*) - search query
    * **status**\ (*string*) - one of "Queued", "Completed", "In progress", "Failure", "Aborting", "Aborted"
    * **records**\ (*object*) - list of record
    * **columns**\ (*object*) - list of columns
    * **extraColumns**\ (*object*) - list of extra columns


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
