import os, shutil, pytest, random, time
import pyslabs

here = os.path.dirname(__file__)
prjdir = os.path.join(here, "workdir")
workdir = os.path.join(prjdir, "slabs")
slabfile = os.path.join(prjdir, "test.slab")
npslabsdir = os.path.join(here, "numpy_slabs")

NPROCS = 3
NSIZE = 10
NITER = 5

#@pytest.fixture(autouse=True)
def run_around_tests():

    # before test
    if os.path.isdir(workdir):
        shutil.rmtree(workdir)
 
    if os.path.isfile(slabfile):
        os.remove(slabfile)

    # the test
    yield


    # after test
    if os.path.isfile(slabfile):
        os.remove(slabfile)


def generate_randomkey(shape):

    key = []
    for length in shape:

        for _ in range(100):
            nwrites = random.randint(1, 5)

            start = random.randrange(-length, length)
            stop = random.randrange(-length, length)

            step = random.randrange(-length, length)
            while step == 0:
                step = random.randrange(-length, length)
                
            if len([e for e in range(start, stop, step)]) != 0:
                break

        key.append(slice(start, stop, step))

    return tuple(key)


def writelist(myid):

    slabs = pyslabs.parallel_open(slabfile)
    testvar = slabs.get_writer("test", mode="w")

    for i in range(NITER):
        mylist = [(myid, i)]*NSIZE
        testvar.write(mylist, myid*NSIZE)

    slabs.close()


def writenumpy(myid, pdata, start, nwrites):

    slabs = pyslabs.parallel_open(slabfile, mode="w")
    ndata = slabs.get_writer("ndata")

    for _ in range(nwrites):
        ndata.write(pdata, start)

    slabs.close()


def ttest_list():
       
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

    assert myvar.shape == (5, 5, 3)

    myarr1 = myvar[1, :, :]
    assert myarr1 == data1

    myarr2 = myvar[2, 1:4:2, 1:]
    assert myarr2 == [[35,36], [41,42]]


def ttest_numpy():

    try:
        import numpy as np

    except Exception as err:
        print("No numpy module is fould. Test is skipped.")
        return

    data = np.arange(100).reshape((5, 4, 5))

    with pyslabs.open(slabfile, "w") as slabs:
        myvar = slabs.get_writer("myvar")
        for i in range(5):
            myvar.write(data[i, :, :])

    assert os.path.isfile(slabfile)

    with pyslabs.open(slabfile, "r") as slabs:
        myvar = slabs.get_reader("myvar")

    assert myvar.shape == (5, 4, 5)

    myarr1 = myvar[1, :, :]
    assert np.all(myarr1 == data[1,:,:])

    myarr2 = myvar[2, 1:4:2, 1:]
    assert np.all(myarr2 == data[2, 1:4:2, 1:])


def ttest_multiprocessing():
    from multiprocessing import Process

    slabs = pyslabs.master_open(slabfile, NPROCS, mode="w")

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

    slabs = pyslabs.open(slabfile, workdir=workdir, mode="r")
    var = slabs.get_reader("test")
    data = slabs.get_array("test")

    slabs.close()

    data1 = data[1]
    arr1 = var[1,:,:]

    assert arr1 == data1

    data2 = data[2][-1]
    arr2 = var[2, -1]

    assert arr2 == data2



def test_random():
    from multiprocessing import Process

    try:
        import numpy as np

    except Exception as err:
        print("No numpy module is fould. Test is skipped.")
        return

    NTESTS = 100
    NSUBTESTS = 100
    count = 0

    while (count < NTESTS):

        # before test
        if os.path.isdir(workdir):
            shutil.rmtree(workdir)
     
        if os.path.isfile(slabfile):
            os.remove(slabfile)

        # for generating data
        ndim = random.randint(1, 5)
        shape = random.choices(range(1, 7), k=ndim)
        nwrites = random.randint(1, 5)

        print("")
        print("SHAPE: %s" % str(shape))
        print("NWRITES: %s" % str(nwrites))

        # for generating particular shape
        #shape = [3,5]
        #nwrites = 5
        #ndim = len(shape)

        _data = np.arange(np.prod(shape)).reshape(shape)

        if nwrites > 1:
            _d = []
            for _ in range(nwrites):
                _d.append(_data)
            data = np.stack(_d, axis=0)
        else:
            data = _data

        s0 = shape[0]
        shape[0] *= NPROCS

        slabs = pyslabs.master_open(slabfile, NPROCS, mode="w")

        ndata = slabs.get_writer("ndata")

        procs = []

        for i in range(NPROCS-1):
            start = (i+1)*s0
            stop = (i+2)*s0
            pdata = _data[start:stop]
            pshape = [0]*ndim; pshape[0] = start
            p = Process(target=writenumpy, args=(i+1, pdata, start, nwrites))
            p.start()
            procs.append(p)

        slabs.begin()

        for _ in range(nwrites):
            ndata.write(_data[:s0], start=0)

        for i in range(NPROCS-1):
            procs[i].join()

        slabs.close()

        with pyslabs.open(slabfile) as slabs:
            outvar = slabs.get_reader("ndata", pack_stack_dim=True)
            outdata = slabs.get_array("ndata", pack_stack_dim=True)

# For debug
# TODO check elements and subarrays
        if not np.all(data == outdata):
            print("FAIL: get_array mismatch")
            import pdb; pdb.set_trace()


        if not np.all(data[0] == outvar[0]):
            print("FAIL: get_var mismatch")
            import pdb; pdb.set_trace()

        subcount = 0

        while subcount < NSUBTESTS:

            key = generate_randomkey(data.shape)

            print("KEY: ", str(key))

            try:
                if not np.all(data.__getitem__(key) == outdata.__getitem__(key)):
                    print("FAIL: (data, outdata)  mismatch")
                    import pdb; pdb.set_trace()

                if not np.all(data.__getitem__(key) == outvar.__getitem__(key)):
                    print("FAIL: (data, outvar)  mismatch")
                    import pdb; pdb.set_trace()

                if not np.all(outdata.__getitem__(key) == outvar.__getitem__(key)):
                    print("FAIL: (outdata, outvar)  mismatch")
                    import pdb; pdb.set_trace()

            except TypeError as err:
                import pdb; pdb.set_trace()

            subcount += 1
            
        print("PASS")

        count += 1

        # after test
        if os.path.isfile(slabfile):
            os.remove(slabfile)


def ttest_failecases():
    from multiprocessing import Process

    try:
        import numpy as np

    except Exception as err:
        print("No numpy module is fould. Test is skipped.")
        return

    for item in os.listdir(npslabsdir):
        slabfile = os.path.join(npslabsdir, item)
        basename, ext = os.path.splitext(item)

        if ext != ".slab":
            continue

        shape = [int(i) for i in basename.split("_")]
        #if shape != [1,6,4,3]: continue
        #if shape == [1,6,4,3]: import pdb; pdb.set_trace()

        ndim = len(shape)
        s1 = shape[1] // NPROCS
        print("\nshape: %s" % str(shape))

        _data = np.arange(np.prod(shape[1:])).reshape(shape[1:])

        if shape[0] > 1:
           data = np.stack([_data] * shape[0])

        else:
            data = _data 

        with pyslabs.open(slabfile) as slabs:
            outvar = slabs.get_reader("ndata", pack_stack_dim=True)
            outdata = slabs.get_array("ndata", pack_stack_dim=True)

        if not np.all(data == outdata):
            #print("")
            #where = np.where(data != outdata) 
            #print(len(where), [len(i) for i in where])
            #print(len(where), len(where[0]))
            #import pdb; pdb.set_trace()
            print("FAIL: get_array mismatch")
            assert False
        else:
            print("PASS: get_array match")

        if not np.all(data[0] == outvar[0]):
            import pdb; pdb.set_trace()
            print("FAIL: get_var mismatch")
            assert False
        else:
            print("PASS: get_var match")

# TODO: random, stress test
