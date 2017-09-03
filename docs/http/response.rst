
Response JSON format
-----------------------


**SQLite datatable response**::

    {
        "id": "hoge",
        "format": "datatable",
        "progress": 100
    }


:Attributes:
    * **id**\ (*string*) - datatable ID
    * **name**\ (*string*) - datatable name
    * **format**\ (*string*) - "datatable"
    * **query**\ (*object*) - query JSON object
    * **fields**\ (*object*) - list of fields
    * **records**\ (*object*) - list of record
    * **status**\ (*string*) - task status
    * **created**\ (*string*) - date computation job created
    * **resultCount**\ (*int*) - total number of result rows
    * **taskCount**\ (*int*) - total number of row tasks to be processed
    * **doneCount**\ (*int*) - total number of done tasks
    * **progress**\ (*float*) - doneCount / taskCount * 100

    * **responseDate**\ (*string*) - date of response sent from server
    * **execTime**\ (*float*) - execution time


**Error response**::

    {
        "id": "hoge",
        "status": "failure",
        "message": "Job not found"
    }


:Attributes:
    * **id**\ (*string*) - datatable ID
    * **status**\ (*string*) - status
    * **message**\ (*string*) - error message


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
