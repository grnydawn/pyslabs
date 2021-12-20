import os, shutil
import pyslabs

here = os.path.dirname(__file__)
prjdir = os.path.join(here, "workdir")
workdir = os.path.join(prjdir, "slabs")
slabfile = os.path.join(prjdir, "test.slab")

NPROCS = 3
NSIZE = 10
NITER = 5

def writelist(myid):

    slabs = pyslabs.parallel_open(slabfile, mode="w")
    testvar = slabs.get_writer("test")

    for i in range(NITER):
        mylist = [(myid, i)]*NSIZE
        testvar.write(mylist, (myid*NSIZE, 0), shape=(NSIZE, 2))

    slabs.close()


def test_serial():

    if os.path.isdir(workdir):
        shutil.rmtree(workdir)

    if os.path.isfile(slabfile):
        os.remove(slabfile)


    slabs = pyslabs.master_open(slabfile, workdir=workdir, mode="w")

    testvar = slabs.get_writer("test", shape=(True, 10, 2))

    for i in range(NITER):
        mylist = [(0, i)]*NSIZE
        testvar.write(mylist, (0, 0), shape=(10, 2))

    slabs.close()

    slabs = pyslabs.master_open(slabfile, workdir=workdir, mode="r")
    data = slabs.get_array("test")

    slabs.close()

    assert len(data) == NITER
    assert all([len(slab)==NSIZE for slab in data])
    assert all([len(slab)==2 for slab in data[0]])
    assert all([sum(slab[1])==i for i, slab in enumerate(data)])

    os.remove(slabfile)


def test_multiprocessing():
    from multiprocessing import Process

    if os.path.isdir(workdir):
        shutil.rmtree(workdir)

    if os.path.isfile(slabfile):
        os.remove(slabfile)

    slabs = pyslabs.master_open(slabfile, mode="w", nprocs=NPROCS)

    procs = []

    for i in range(NPROCS-1):
        p = Process(target=writelist, args=(i+1,))
        p.start()
        procs.append(p)

    testvar = slabs.get_writer("test", shape=(NITER, NSIZE*NPROCS, 2))

    slabs.begin()

    for i in range(NITER):
        mylist = [(0, i)]*NSIZE
        testvar.write(mylist, (0, 0), shape=(NSIZE, 2))

    for i in range(NPROCS-1):
        procs[i].join()

    slabs.close()

    slabs = pyslabs.master_open(slabfile, workdir=workdir, mode="r")
    var = slabs.get_reader("test")
    data = slabs.get_array("test")

    var2 = var[1,:,:]

    slabs.close()

    assert len(data) == NITER
    assert all([len(slab)==NSIZE*NPROCS for slab in data])
    assert all([len(slab)==2 for slab in data[0]])
    assert data[NITER-1][NSIZE*NPROCS-1] == (NPROCS-1, NITER-1)

    os.remove(slabfile)
