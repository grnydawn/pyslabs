import os, shutil
import pyslabs

here = os.path.dirname(__file__)
workdir = os.path.join(here, "workdir", "tests")

NPROCS = 4
NSIZE = 10
NITER = 5

def f(x):
    return x*x

def writelist(myid):

    slabs = pyslabs.parallel_open(workdir, mode="w")
    testvar = slabs.get_var("test")

    for i in range(NITER):
        mylist = [(myid, i)]*NSIZE
        testvar.write(mylist, myid*NSIZE)

    slabs.close()


def test_serial():

    if os.path.isdir(workdir):
        shutil.rmtree(workdir)

    slabs = pyslabs.master_open(workdir, mode="w")

    testvar = slabs.define_var("test")

    for i in range(NITER):
        mylist = [(0, i)]*NSIZE
        testvar.write(mylist, 0)

    slabs.close()

    slabs = pyslabs.master_open(workdir, mode="r")
    data = slabs.get_array("test")

    assert len(data) == NITER
    assert all([len(slab)==NSIZE for slab in data])
    assert all([sum(slab[1])==i for i, slab in enumerate(data)])

    slabs.close()


def test_multiprocessing():
    from multiprocessing import Process

    if os.path.isdir(workdir):
        shutil.rmtree(workdir)

    procs = []

    for i in range(NPROCS-1):
        p = Process(target=writelist, args=(i+1,))
        p.start()
        procs.append(p)

    slabs = pyslabs.master_open(workdir, mode="w", nprocs=NPROCS)

    testvar = slabs.define_var("test")

    slabs.begin()

    for i in range(NITER):
        mylist = [(0, i)]*NSIZE
        testvar.write(mylist, 0)

    slabs.close()

    for i in range(NPROCS-1):
        procs[i].join()

    slabs = pyslabs.master_open(workdir, mode="r")
    data = slabs.get_array("test")

    #import pdb; pdb.set_trace()
    assert len(data) == NITER
    assert all([len(slab)==NSIZE*NPROCS for slab in data])
    assert data[NITER-1][NSIZE*NPROCS-1] == (NPROCS-1, NITER-1)

