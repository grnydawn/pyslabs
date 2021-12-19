.. pyslabs documentation master file, created by
   sphinx-quickstart on Sat Dec  4 16:54:01 2021.

.. only:: html or man

   :Version:  |release|
   :Date:     |today|

pyslabs
===========

pyslabs is a pure Python parallel I/O module.

Features
----------

* Create arrays with pickle-able objects.
* Create arrays with famous data formats such as Numpy.
* Support serial, multiprocessing, and MPI programming.
* Read only the parts of an array per requests(under development).
* Unload parts of an array to reduce main memory usage(under development).

Notice
------

pyslabs is an experimental project as of this version.


Contents
--------

.. toctree::
    :maxdepth: 2

    install
    tutorial
    api
    tools


Acknowledgments
---------------

This page is inspired from `Zarr <https://zarr.readthedocs.io/en/stable/index.html/>`_ documentation.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
