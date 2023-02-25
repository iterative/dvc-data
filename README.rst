DVC data
========

|PyPI| |Status| |Python Version| |License|

|Tests| |Codecov| |pre-commit| |Black|

.. |PyPI| image:: https://img.shields.io/pypi/v/dvc-data.svg
   :target: https://pypi.org/project/dvc-data/
   :alt: PyPI
.. |Status| image:: https://img.shields.io/pypi/status/dvc-data.svg
   :target: https://pypi.org/project/dvc-data/
   :alt: Status
.. |Python Version| image:: https://img.shields.io/pypi/pyversions/dvc-data
   :target: https://pypi.org/project/dvc-data
   :alt: Python Version
.. |License| image:: https://img.shields.io/pypi/l/dvc-data
   :target: https://opensource.org/licenses/Apache-2.0
   :alt: License
.. |Tests| image:: https://github.com/iterative/dvc-data/workflows/Tests/badge.svg
   :target: https://github.com/iterative/dvc-data/actions?workflow=Tests
   :alt: Tests
.. |Codecov| image:: https://codecov.io/gh/iterative/dvc-data/branch/main/graph/badge.svg
   :target: https://app.codecov.io/gh/iterative/dvc-data
   :alt: Codecov
.. |pre-commit| image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white
   :target: https://github.com/pre-commit/pre-commit
   :alt: pre-commit
.. |Black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
   :alt: Black


Features
--------

* TODO


Requirements
------------

* TODO


Installation
------------

You can install *DVC data* via pip_ from PyPI_:

.. code:: console

   $ pip install dvc-data


Usage
-----

HashFile
^^^^^^^^

HashFile
""""""""

Based on dvc-object's `Object`, this is an object that has a particular hash that can be used to verify its contents. Similar to git's `ShaFile`.

.. code:: python

    from dvc_data.hashfile import HashFile

    obj = HashFile("/path/to/file", fs, HashInfo("md5", "36eba1e1e343279857ea7f69a597324e")

HashFileDB
""""""""""

Based on dvc-object's `ObjectDB`, but stores `HashFile` objects and so is able to verify their contents by their `hash_info`. Similar to git's `ObjectStore`.

.. code:: python

    from dvc_data.hashfile import HashFileDB

    odb = HashFileDB(fs, "/path/to/odb")

Index
^^^^^

Index
"""""

A trie-like structure that represents data files and directories.

.. code:: python

    from dvc_data.index import DataIndex, DataIndexEntry

    index = DataIndex()
    index[("foo",)] = DataIndexEntry(hash_info=hash_info, meta=meta)


Storage
"""""""

A mapping that describes where to find data contents for index entries. Can be either `ObjectStorage` for `HashFileDB`-based storage or `FileStorage` for backup-like plain file storage.

.. code:: python

    index.storage_map[("foo",)] = ObjectStorage(...)

Contributing
------------

Contributions are very welcome.
To learn more, see the `Contributor Guide`_.


License
-------

Distributed under the terms of the `Apache 2.0 license`_,
*DVC data* is free and open source software.


Issues
------

If you encounter any problems,
please `file an issue`_ along with a detailed description.


.. _Apache 2.0 license: https://opensource.org/licenses/Apache-2.0
.. _PyPI: https://pypi.org/
.. _file an issue: https://github.com/iterative/dvc-data/issues
.. _pip: https://pip.pypa.io/
.. github-only
.. _Contributor Guide: CONTRIBUTING.rst
