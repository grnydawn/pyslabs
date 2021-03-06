import os, shutil, pytest
import pyslabs
import numpy as np

here = os.path.dirname(__file__)
prjdir = os.path.join(here, "workdir")
workdir = os.path.join(prjdir, "slabs")
slabfile = os.path.join(prjdir, "test.slab")

NPROCS = 3
NSIZE = 10
NITER = 5


@pytest.fixture(autouse=True)
def run_around_tests():

    # before test
    if os.path.isdir(workdir):
        shutil.rmtree(workdir)
 
    if os.path.isfile(slabfile):
        os.remove(slabfile)

    # the test
    yield


    # after test
    os.remove(slabfile)

def writelist(myid):

    slabs = pyslabs.parallel_open(slabfile)
    testvar = slabs.get_writer("test")

    slabs.begin()

    for i in range(NITER):
        mylist = [(myid, i)]*NSIZE
        testvar.write(mylist, start=(myid*NSIZE, 0))
        testvar.stacking()

    slabs.close()


def test_serial():

    slabs = pyslabs.open(slabfile, workdir=workdir, mode="w")

    testvar = slabs.get_writer("test", (NITER, NSIZE, 2))

    for i in range(NITER):
        mylist = [(0, i)]*NSIZE
        testvar.write(mylist, (0, 0))
        testvar.stacking()

    slabs.close()

    slabs = pyslabs.open(slabfile, workdir=workdir)
    reader = slabs.get_reader("test")
    data = slabs.get_array("test")
    slabs.close()

    assert len(data) == NITER
    assert all([len(slab)==NSIZE for slab in data])
    assert all([len(slab)==2 for slab in data[0]])
    assert all([sum(slab[1])==i for i, slab in enumerate(data)])

    subdata = reader[1]
    assert len(subdata) == NSIZE
    assert all([len(slab)==2 for slab in subdata])
    assert all([slab==(0,1) for slab in subdata])


def test_multiprocessing():
    from multiprocessing import Process

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

    slabs = pyslabs.open(slabfile, workdir=workdir, mode="r")
    var = slabs.get_reader("test")
    data = slabs.get_array("test")

    slabs.close()

    assert len(data) == NITER
    assert all([len(slab)==NSIZE*NPROCS for slab in data])
    assert all([len(slab)==2 for slab in data[0]])
    assert data[NITER-1][NSIZE*NPROCS-1] == (NPROCS-1, NITER-1)

    assert np.array_equal(var[1,:,:], np.asarray(data)[1,:,:])
    assert np.array_equal(var[1,:,0], np.asarray(data)[1,:,0])
    assert np.array_equal(var[1,0,:], np.asarray(data)[1,0,:])
    assert np.array_equal(var[:,:,1], np.asarray(data)[:,:,1])
    assert np.array_equal(var[1:3,3:6,:], np.asarray(data)[1:3,3:6,:])

