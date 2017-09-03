
Query API
=======================

**SQLite datatable query**::

    {
        "type": "chemsearch",
        "targets": ["drugbankfda"],
        "key": "id",
        "values": ["DB00189", "DB00193", "DB00203", "DB00865", "DB01143"]
    }

:Attributes:
    * **type**\ (*string*) - query task type
    * **targets**\ (*object*) - list of target resource ID
    * **targetFields**\ (*object*) - list of target resource field keys
    * **key**\ (*string*) - key
    * **values**\ (*object*) - list of values
    * **operator**\ (*string*) - query operator
    * **queryMol**\ (*object*) - query molecule object
        * **format**\ (*string*) - "dbid", "molfile" or "smiles"
        * **source**\ (*string*) - (if format is "dbid") target resource ID
        * **value**\ (*string*) - Compound ID, MOLFile text or SMILES text
    * **params**\ (*object*) - optional parameters
        * **ignoreHs**\ (*bool*) - ignore hydrogens in substructure match
        * **measure**\ (*int*) - similarity measure
        * **threshold**\ (*int*) - similarity threshold
        * **diameter**\ (*int*) - MCS-DR diameter
        * **maxTreeSize**\ (*int*) - MCS-DR max tree size
        * **molSizeCutoff**\ (*int*) - MCS-DR molecule size cutoff
        * **timeout**\ (*float*) - timeout for rdkit FMCS


**SDFile import query (POST)**::

:Attributes:
    * **contents**\ (*file*) - SDFile contents (binary)
    * **params**\ (*object*) - optional parameters
        * **fields**\ (*object*) - list of fields to be imported
        * **implh**\ (*bool*) - make hydrogens implicit or not
        * **recalc**\ (*bool*) - recalculate 2D coordinates or not


**SDFile export query (POST)**::

:Attributes:
    * **contents**\ (*file*) - JSON datafile (binary, stringified)


**Similarity network query (POST)**::

:Attributes:
    * **contents**\ (*file*) - JSON datafile (binary, stringified)
    * **params**\ (*object*) - optional parameters


**Job result query (GET)**::

    {
        "id": "",
        "command": "abort"
    }

:Attributes:
    * **id**\ (*string*) - job ID
    * **command**\ (*string*) - command to the server task



* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
