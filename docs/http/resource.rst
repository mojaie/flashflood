
Resource schema
===========================

.. code-block:: python

    "resources": [
        {
            "id": "test_db",
            "name": "Test DB",
            "domain": "chemical",
            "entity": "chemdb:library1",
            "columns": [
                {
                    "key": "ID",
                    "sort": "text"
                },
                {
                    "key": "mw",
                    "name": "Molecular weight",
                    "sort": "numeric",
                    "request": "job"
                },
                {
                    "key": "struct",
                    "name": "Structure",
                    "sort": "none"
                }
            ]
            "description": "This is Test DB.",
        }
    ]


:Resource:
    * **id**\ (*string*) - table ID for annotation (experiment ID)
    * **domain**\ (*string*) - identifies group of dbs and site of external resources
    * **entity**\ (*string*) - db table name(refID:layer or db:table)
    * **name**\ (*string*) - display name of the table
    * **tags**\ (*object*) - list of annotation key
    * **description**\ (*string*) - optional description
    * **columns**\ (*object*) - list of column

:Column:
    * **key**\ (*string*) - column key
    * **name**\ (*string*) - display name of the column
    * **sort**\ (*string*) - one of "numeric", "text", "none"
    * **dataType**\ (*string*) - data type (ex. inhibition%, IC50, annotation flag, ...)
    * **request**\ (*string*) - if "job", the value is not stored on the db and  will be calculated on the fly
    * **tags**\ (*object*) - list of annotation key. if not specified, it will be the duplicate of table tags


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
