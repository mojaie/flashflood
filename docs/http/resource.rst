
Resource schema
===========================

.. code-block:: python

    "resources": [
        {
            "id": "test_db",
            "name": "Test DB",
            "domain": "chemical",
            "resourceType": "sqlite",
            "resourceFile": "test.sqlite3",
            "fields": [
                {
                    "key": "compound_id",
                    "name": "ID", "valueType": "compound_id"
                },
                {
                    "key": "mw",
                    "name": "Molecular weight", "valueType": "numeric"
                },
                {
                    "key": "_structure",
                    "name": "Structure", "valueType": "svg"
                }
            ]
            "description": "This is Test DB.",
        }
    ]


:Resource:
    * **id**\ (*string*) - table ID for annotation (experiment ID)
    * **name**\ (*string*) - display name of the table
    * **domain**\ (*string*) - data type category
    * **resourceType**\ (*string*) - resource media category
    * **resourceFile**\ (*string*) - resource file path
    * **resourceURL**\ (*string*) - resource file URL
    * **description**\ (*string*) - optional description
    * **fields**\ (*object*) - list of fields

:fields:
    * **key**\ (*string*) - column key
    * **name**\ (*string*) - display name of the column
    * **valueType**\ (*string*) - value type category
    * **sortType**\ (*string*) - sort order (numeric, text or none(unsortable))


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
