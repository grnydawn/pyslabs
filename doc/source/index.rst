.. pyslabs documentation master file, created by
   sphinx-quickstart on Sat Dec  4 16:54:01 2021.

.. only:: html or man

   :Version:  |release|
   :Date:     |today|

pyslabs
===========

pyslabs is a pure Python parallel I/O module.

Highlights
----------

* Create arrays with any pickle-able objects.
* Create arrays with famous data formats such as Numpy.
* Support serial, multiprocessing, and MPI programming.
* Write quickly using task-local files.
* Read only the parts of an array per requests(under development).
* Unload parts of an array to reduce main memory usage(under development).

Status
------

pyslabs is an experimental project as of this version.

Installation
------------

Install pyslabs from PyPI::

    $ pip install pyslabs


To install the latest development version of pyslabs, you can use pip with the
latest GitHub master::

    $ pip install git+https://github.com/grnydawn/pyslabs.git

To work with pyslabs source code in development, install from GitHub::

    $ git clone https://github.com/grnydawn/pyslabs.git
    $ cd pyslabs
    $ python setup.py install

To verify pyslabs installation, run the pyslabs command-line tool::

    $ slabs --version # under development

Contents
--------

.. toctree::
    :maxdepth: 2

    tutorial
    api
    slabs


Acknowledgments
---------------

This page is inspired from `Zarr <https://zarr.readthedocs.io/en/stable/index.html/>`_ documentation.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
