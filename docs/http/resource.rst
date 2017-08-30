
Resource API
===========================


:Domain:
    * **domain**\ (*string*) - API ID
    * **name**\ (*string*) - display name of the API domain
    * **description**\ (*string*) - optional description
    * **tables**\ (*object*) - list of tables

:Table:
    * **id**\ (*string*) - table ID for annotation (experiment ID)
    * **entity**\ (*string*) - db table name(refID:layer or db:table)
    * **name**\ (*string*) - display name of the table
    * **tags**\ (*object*) - list of annotation key
    * **description**\ (*string*) - optional description
    * **columns**\ (*object*) - list of column

:Column:
    * **key**\ (*string*) - column key for annotation
    * **dataColumn**\ (*string*) - db table column name
    * **name**\ (*string*) - display name of the column
    * **sort**\ (*string*) - one of "numeric", "text", "none"
    * **request**\ (*string*) - if "calc", the value will be calculated on request
    * **tags**\ (*object*) - list of annotation key. if not specified, table tags are copied to the column tags


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
