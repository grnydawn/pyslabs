import os, shutil
import pyslabs

here = os.path.dirname(__file__)
prjdir = os.path.join(here, "workdir")
workdir = os.path.join(prjdir, "slabs")
slabfile = os.path.join(prjdir, "test.slab")

NPROCS = 3
NSIZE = 10
NITER = 5

def f(x):
    return x*x

def writelist(myid):

    slabs = pyslabs.parallel_open(slabfile, mode="w")
    testvar = slabs.get_writer("test")

    for i in range(NITER):
        mylist = [(myid, i)]*NSIZE
        testvar.write(mylist, myid*NSIZE)

    slabs.close()


def ttest_list():

    if os.path.isdir(workdir):
        shutil.rmtree(workdir)
 
    if os.path.isfile(slabfile):
        os.remove(slabfile)
       
    data0 = [[ 1, 2, 3], [ 4, 5, 6], [ 7, 8, 9], [10,11,12], [13,14,15]]
    data1 = [[16,17,18], [19,20,21], [22,23,24], [25,26,27], [28,29,30]]
    data2 = [[31,32,33], [34,35,36], [37,38,39], [40,41,42], [43,44,45]]
    data3 = [[46,47,48], [49,50,51], [52,53,54], [55,56,57], [58,59,60]]
    data4 = [[61,62,63], [64,65,66], [67,68,69], [70,71,72], [73,74,75]]

    with pyslabs.open(slabfile, "w") as slabs:
        myvar = slabs.get_writer("myvar")
        myvar.write(data0)
        myvar.write(data1)
        myvar.write(data2)
        myvar.write(data3)
        myvar.write(data4)

    assert os.path.isfile(slabfile)

    with pyslabs.open(slabfile, "r") as slabs:
        myvar = slabs.get_reader("myvar")

    assert myvar.ndim == 3
    assert myvar.shape == (5, 5, 3)

    myarr1 = myvar[1, :, :]
    assert myarr1 == data1

    myarr2 = myvar[2, 1:4:2, 1:]
    assert myarr2 == [[35,36], [41,42]]

    os.remove(slabfile)


def ttest_numpy():

    try:
        import numpy as np

    except Exception as err:
        print("No numpy module is fould. Test is skipped.")
        return

    data = np.arange(100).reshape((5, 4, 5))

    if os.path.isdir(workdir):
        shutil.rmtree(workdir)
 
    if os.path.isfile(slabfile):
        os.remove(slabfile)

    with pyslabs.open(slabfile, "w") as slabs:
        myvar = slabs.get_writer("myvar")
        for i in range(5):
            myvar.write(data[i, :, :])

    assert os.path.isfile(slabfile)

    with pyslabs.open(slabfile, "r") as slabs:
        myvar = slabs.get_reader("myvar")

    assert myvar.ndim == 3
    assert myvar.shape == (5, 4, 5)

    myarr1 = myvar[1, :, :]
    assert np.all(myarr1 == data[1,:,:])

    myarr2 = myvar[2, 1:4:2, 1:]
    assert np.all(myarr2 == data[2, 1:4:2, 1:])

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

    slabs.close()

    data1 = data[1]
    arr1 = var[1,:,:]

    assert arr1 == data1

    data2 = data[1:3][-1][1]
    import pdb; pdb.set_trace()

    # TODO: complete one level  and move to the next level with the same logic
    arr2 = var[1:3, -1, 1]

    assert arr2 == data2

    os.remove(slabfile)

# TODO: mpi
# TODO: random, stress test
