
import os
import numpy
from multiprocessing import Process
import pyslabs

here = os.path.dirname(__file__)
slabfile = os.path.join(here, "test.slab")
NPROCS = 3
NELEMS = 4

# for parallel execution
def func(myid):

    slabs = pyslabs.parallel_open(slabfile)

    testvar = slabs.get_writer("test")

    # arguments: (array, starting index)
    testvar.write(numpy.ones(NELEMS)*myid, myid*NELEMS)

    slabs.close()

def main():
    procs = []

    # for master process
    slabs = pyslabs.master_open(slabfile, mode="w", num_procs=NPROCS)

    testvar = slabs.get_writer("test", (NELEMS,))

    for i in range(NPROCS-1):
        p = Process(target=func, args=(i,))
        p.start()
        procs.append(p)

    # should run after running child processes
    slabs.begin()

    # arguments: (array, starting index)
    testvar.write(numpy.ones(NELEMS)*(NPROCS-1), NELEMS*(NPROCS-1))

    slabs.close()

    with pyslabs.open(slabfile, "r") as slabs:
        data = slabs.get_array("test")

    print(type(data))
    print(data)

    os.remove(slabfile)


if __name__ == "__main__":
    main()
