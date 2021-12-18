.. _tutorial:

Tutorial
========

Pyslabs makes it easy for Python programmers to work with data in the whole data life-cycle.

.. _tutorial_filecreate:

Generating a slab file
--------------------------

Open a file using Pyslab context manager::

    >>> import pyslab
    >>> data0 = (1,2,3)
    >>> data1 = (4,5,6)
    >>>
    >>> with pyslabs.open("mydata.slab", "w") as slabs:
    ...     myvar = slabs.define_var("myvar")
    ...     myvar.write(data0)
    ...     myvar.write(data1)
    ... 

In this code, a slab file named "mydata.slab" is created with a familar method name of "open". A "slab" is the name of the data format used in Pyslabs. In the file, a variable named "myvar" is added by using "define_var" method of the context manager. Finally, two tuples are saved in the variable one by one.

Reading a slab file
--------------------------

Now, let's examine the slab file generated at the previous step::

    >>> with pyslabs.open("mydata.slab", "r") as slabs:
    >>>     myarr = slabs.get_array("myvar")
    >>> myarr
    ((1, 2, 3), (4, 5, 6))

As you may guess, "get_array" method of the context manager, returns the array that was saved under the variable name of "myvar."  The two tuples in the variable are stacked in the order of the writes.


Using Pyslabs in multiprocessing
---------------------------------

From the beginnig, Pyslabs is desinged to work in multiprocessing environment. First, let me take an example of using Pyslabs on multiple processes created by Python multiprocessing module. I left only the code lines relevant to Pyslabs here. Full code can be found in "examples" directory of Pyslabs Github repository::

    >>> # for parallel execution
    >>> def func(myid):
    >>>     slabs = pyslabs.parallel_open(slabfile, mode="w")
    >>> 
    >>>     testvar = slabs.get_var("test")
    >>>     # arguments: (array, starting index)
    >>>     testvar.write(numpy.ones(NELEMS)*myid, myid*NELEMS)
    >>> 
    >>>     slabs.close()
    >>> 
    >>> for i in range(NPROCS-1):
    >>>     p = Process(target=func, args=(i,))
    >>>
    >>> # for master process
    >>> slabs = pyslabs.master_open(slabfile, mode="w", nprocs=NPROCS)
    >>> testvar = slabs.define_var("test")
    >>>
    >>> slabs.begin()
    >>> 
    >>> # arguments: (array, starting index)
    >>> testvar.write(numpy.ones(3)*(NPROCS-1), NPROCS-1)
    >>> 
    >>> slabs.close()
    >>> 
    >>> with pyslabs.master_open(slabfile, mode="r") as slabs:
    >>>     data = slabs.get_array("test")
    >>>
    >>> type(data)
    <class 'numpy.ndarray'>
    >>> data
    [0. 0. 0. 1. 1. 1. 2. 2. 2.]


Using Pyslabs with MPI
----------------------

Pyslabs can work in distributed computing environment such as MPI. I left only the code lines relevant to Pyslabs here. Full code can be found in "tests" directory of Pyslabs Github repository::

    >>>



(Almost) unlimited data type supports
----------------------------------------

Pyslabs works transparently in terms of data format. Pyslabs does have a thin layer of interface to well-known frameworks such as Numpy. In case that Pyslabs can not find a proper interface for a data, it falls back to Python pickle. Therefore Pyslabs can support any pickle-able object as a data format::

    >>>
