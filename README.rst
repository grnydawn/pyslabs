##############
PySlabs
##############

PySlabs is a pure Python parallel I/O module.

Features
----------

* Create arrays with pickle-able objects.
* Create arrays with famous data formats such as Numpy.
* Support serial, multiprocessing, and MPI programming.
* Read only the parts of an array per requests.
* Unload parts of an array to reduce main memory usage(under development).

**pyslabs is an experimental project as of this version.**

Installation
------------

You can use pip to install pyslabs from the Python package index(stable) or from the pyslabs github repository(latest)::

    $ pip install pyslabs
    $ pip install git+https://github.com/grnydawn/pyslabs.git

To work with pyslabs source code, use git to download from the github repository::

    $ git clone https://github.com/grnydawn/pyslabs.git
    $ cd pyslabs
    $ python setup.py install

To verify pyslabs installation, run the pyslabs command-line tool::

    $ slabs --version



Generating a slab file
--------------------------

Open a file using Pyslab context manager::

    >>> import pyslabs
    >>> data0 = (1,2,3)
    >>> data1 = (4,5,6)
    >>>
    >>> with pyslabs.open("mydata.slab", mode="w") as slabs:
    ...     myvar = slabs.get_writer("myvar", autostack=True)
    ...     myvar.write(data0)
    ...     myvar.write(data1)
    ...

In this code, a slab file named "mydata.slab" is created with a familar method name of "open". A "slab" is the name of the data format used in Pyslabs. In the file, a variable named "myvar" is added by using "define_var" method of the context manager. Finally, two tuples are saved in the variable one by one.

Reading a slab file
--------------------------

Now, let's examine the slab file generated at the previous step::

    >>> with pyslabs.open("mydata.slab", mode="r") as slabs:
    >>>     myarr = slabs.get_array("myvar")
    >>> myarr
    ((1, 2, 3), (4, 5, 6))

As you may guess, "get_array" method of the context manager, returns the array that was saved under the variable name of "myvar."  The two tuples in the variable are stacked in the order of the writes.



Using Pyslabs in multiprocessing
---------------------------------

From the beginnig, Pyslabs is desinged to work in multiprocessing environment. First, let me take an example of using Pyslabs on multiple processes created by Python multiprocessing module. I left only the code lines relevant to Pyslabs here. Full code can be found in "examples" directory of Pyslabs Github repository::

        def writelist(myid):

            slabs = pyslabs.parallel_open(slabfile)
            testvar = slabs.get_writer("test")

            slabs.begin()

            for i in range(NITER):
                mylist = [(myid, i)]*NSIZE
                testvar.write(mylist, start=(myid*NSIZE, 0))
                testvar.stacking()

            slabs.close()

        slabs = pyslabs.master_open(slabfile, NPROCS)

        testvar = slabs.get_writer("test", (NITER, NSIZE*NPROCS, 2))

        procs = []

        for i in range(NPROCS-1):
        p = Process(target=writelist, args=(i+1,))
        p.start()
        procs.append(p)

        # should be located after child processes began
        slabs.begin()

        for i in range(NITER):
        mylist = [(0, i)]*NSIZE
        testvar.write(mylist, start=(0, 0))
        testvar.stacking()


        for i in range(NPROCS-1):
        procs[i].join()

        slabs.close()


Using Pyslabs with MPI
----------------------

Pyslabs can work in distributed computing environment such as MPI. Please see MPI example at `PyWeather <https://github.com/grnydawn/pyweather/tree/master/miniweather>`_ .

