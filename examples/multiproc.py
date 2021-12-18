
import os
import numpy
from multiprocessing import Process
import pyslabs

here = os.path.dirname(__file__)
slabfile = os.path.join(here, "test.slab")
NPROCS = 3
NELEMS = 3

# for parallel execution
def func(myid):

    slabs = pyslabs.parallel_open(slabfile, mode="w")

    testvar = slabs.get_var("test")

    # arguments: (array, starting index)
    testvar.write(numpy.ones(NELEMS)*myid, myid*NELEMS)

    slabs.close()

def main():
    procs = []

    # for master process
    slabs = pyslabs.master_open(slabfile, mode="w", nprocs=NPROCS)

    testvar = slabs.define_var("test")

    for i in range(NPROCS-1):
        p = Process(target=func, args=(i,))
        p.start()
        procs.append(p)

    # should run after running child processes
    slabs.begin()

    # arguments: (array, starting index)
    testvar.write(numpy.ones(3)*(NPROCS-1), NELEMS*(NPROCS-1))

    slabs.close()

    with pyslabs.master_open(slabfile, mode="r") as slabs:
        data = slabs.get_array("test", squeeze=True)

    print(type(data))
    print(data)

if __name__ == "__main__":
    main()
