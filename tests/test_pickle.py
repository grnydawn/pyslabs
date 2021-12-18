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
    testvar = slabs.get_var("test")

    for i in range(NITER):
        mylist = [(myid, i)]*NSIZE
        testvar.write(mylist, myid*NSIZE)

    slabs.close()


def test_tuple():

    if os.path.isdir(workdir):
        shutil.rmtree(workdir)
 
    if os.path.isfile(slabfile):
        os.remove(slabfile)
       
    data0 = (1,2,3)
    data1 = (4,5,6)

    with pyslabs.open(slabfile, "w") as slabs:
        myvar = slabs.define_var("myvar")
        myvar.write(data0)
        myvar.write(data1)

    assert os.path.isfile(slabfile)

    with pyslabs.open(slabfile, "r") as slabs:
        myarr = slabs.get_array("myvar")

    assert type(myarr) == type(data0)
    assert myarr[0] == data0
    assert myarr[1] == data1

    os.remove(slabfile)
