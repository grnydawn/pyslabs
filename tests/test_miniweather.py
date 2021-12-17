import os, sys, shutil
import numpy as np
import pyslabs

here = os.path.dirname(__file__)
resdir = os.path.join(here, "res")
prjdir = os.path.join(here, "workdir")
workdir = os.path.join(prjdir, "slabs")
slabfile = os.path.join(prjdir, "test.slab")

NPROCS = 3
NSIZE = 10
NITER = 5

def ttest_serial():

    if resdir not in sys.path:
        sys.path.append(resdir)

    from miniweather_serial import main as ser_main

    if os.path.isdir(workdir):
        shutil.rmtree(workdir)

    argv = sys.argv
    sys.argv = [os.path.join(resdir, "miniweather_serial.py"), "-o",
                slabfile]
    ser_main()
    sys.argv = argv

    assert os.path.isfile(slabfile)

    slabs = pyslabs.master_open(slabfile, mode="r")
    dens = slabs.get_array("dens")
    slabs.close()

    assert dens.ndim == 3
    assert not any([x!=0. for x in np.nditer(dens[0])])
    assert not any([x==0. for x in np.nditer(dens[1])])

    os.remove(slabfile)

def test_mpi():

    if resdir not in sys.path:
        sys.path.append(resdir)

    from miniweather_mpi import main as mpi_main

    if os.path.isdir(workdir):
        shutil.rmtree(workdir)

    argv = sys.argv
    sys.argv = [os.path.join(resdir, "miniweather_mpi.py"), "-o",
                slabfile]
    mpi_main()
    sys.argv = argv

    assert os.path.isfile(slabfile)

    slabs = pyslabs.master_open(slabfile, mode="r")
    dens = slabs.get_array("dens")
    slabs.close()

    assert dens.ndim == 3
    assert not any([x!=0. for x in np.nditer(dens[0])])
    assert not any([x==0. for x in np.nditer(dens[1])])

    os.remove(slabfile)

