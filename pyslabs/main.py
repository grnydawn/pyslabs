import os, io, pickle, shutil, time, uuid, tarfile

from pyslabs import wrap


_CONFIG_FILE = "__config__"
_BEGIN_FILE = "__begin__"
_FINISHED = "__finished__"
_EXT = ".slab"
_CEXT = ".zlab"
_BEGIN_EXT = ".__slabbegin__"
_WORKDIR_EXT = ".__slabtmp__"
_MAX_OPEN_WAIT = 10 # seconds
_MAX_CLOSE_WAIT = 100 # seconds
_CONFIG_INIT = {
    "version": 1,
    "dims": {},
    "vars": {},
    "attrs": {},
    "__control__": {
        "nprocs": 1,
    }
} 


class VariableWriter():

    def __init__(self, path, config):

        self.path = path
        self.config = config
        self.writecount = 0

    def write(self, slab, start=None):

        path = self.path

        if start is None:
            start = (0,) * wrap.ndim(slab)

        try:
            for _s in start:
                path = os.path.join(path, str(_s))
                break

        except TypeError:
            path = os.path.join(path, str(start))

        if not os.path.isdir(path):
            os.makedirs(path)

        wc = str(self.writecount)
        atype, ext = wrap.arraytype(slab)
        slabpath = os.path.join(path, ".".join([wc, atype, ext])) 

        wrap.dump(slab, slabpath)

        self.writecount += 1


class VariableReader():

    def __init__(self, path, config):

        self.path = path
        self.config = config


class ParallelPyslabsWriter():

    def __init__(self, root, config):
        self.root = root
        self.uuid = str(uuid.uuid4().hex)
        self.path = os.path.join(self.root, self.uuid)
        self.cfgpath = os.path.join(self.root, _CONFIG_FILE)
        self.config = config

        os.makedirs(self.path)

    def get_var(self, name):

        varcfg = self.config["vars"][name]

        return VariableWriter(os.path.join(self.path, name), varcfg)

    def close(self):

        with io.open(os.path.join(self.path, _FINISHED), "w") as fp:
            fp.write("FINISHED")
            fp.flush()
            os.fsync(fp.fileno())


class MasterPyslabsWriter(ParallelPyslabsWriter):

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.close()

    def begin(self):

        with io.open(self.cfgpath, "wb") as fp:
            pickle.dump(self.config, fp)
            fp.flush()
            os.fsync(fp.fileno())

        procs = []
 
        start = time.time()
        nprocs = self.config["__control__"]["nprocs"]

        while time.time() - start < _MAX_OPEN_WAIT:

            procs.clear()

            for item in os.listdir(self.root):
                if item == self.uuid:
                    procs.append(os.path.join(self.root, item))
                    time.sleep(0.1)
                    continue

                try:
                    if len(item) == len(self.uuid) and int(item, 16):
                        proc = os.path.join(self.root, item)
                        procs.append(proc)

                except ValueError:
                    pass

            if len(procs) == nprocs:
                break

        if len(procs) != nprocs:
            raise Exception("Number of processes mismatch: %d != %d" %
                    (len(procs), nprocs))

    def define_var(self, name):

        varcfg = {}
        self.config["vars"][name] = varcfg

        return VariableWriter(os.path.join(self.path, name), varcfg)

    def close(self):
 
        with io.open(os.path.join(self.path, _FINISHED), "w") as fp:
            fp.write("FINISHED")
            fp.flush()
            os.fsync(fp.fileno())

        beginpath = self.config["__control__"]["beginpath"]

        if os.path.isfile(beginpath):
            os.remove(beginpath)

        def _move_dim(src, dst):
            
            for dim in os.listdir(src):
                srcpath = os.path.join(src, dim)
                dstpath = os.path.join(dst, dim)

                if os.path.isdir(srcpath):
                    if os.path.isdir(dstpath):
                        _move_dim(srcpath, dstpath) 

                    elif os.path.exists(dstpath):
                        raise Exception("Destination path already exists: %s" % dstpath)

                    else:
                        shutil.move(srcpath, dstpath)

                elif os.path.exists(dstpath):
                    raise Exception("Multiple processes creat the same data file: %s" % dstpath)

                else:
                    shutil.move(srcpath, dstpath)
              
        def _move_proc(src, dst):

            for var in os.listdir(src): 
                dstvar = os.path.join(dst, var)
                srcvar = os.path.join(src, var)

                if not os.path.isdir(dstvar):
                    shutil.move(srcvar, dstvar)

                else:
                    _move_dim(srcvar, dstvar) 

        procs = []

        start = time.time()
        nprocs = self.config["__control__"]["nprocs"]

        while time.time() - start < _MAX_CLOSE_WAIT:

            procs.clear()

            for item in os.listdir(self.root):
                if item == self.uuid:
                    procs.append(os.path.join(self.root, item))
                    time.sleep(0.1)
                    continue

                try:
                    if len(item) == len(self.uuid) and int(item, 16):
                        proc = os.path.join(self.root, item)
                        procs.append(proc)

                except ValueError:
                    pass

            if len(procs) == nprocs:
                break

        if len(procs) != nprocs:
            raise Exception("Number of processes mismatch: %d != %d" %
                    (len(procs), nprocs))

        for proc in procs:
            finished = os.path.join(proc, _FINISHED)
            timeout = True

            while time.time() - start < _MAX_CLOSE_WAIT:
                if os.path.isfile(finished):
                    os.remove(finished)
                    timeout = False
                    break
                time.sleep(0.1)

            if timeout:
                raise Exception("Error: timeout on waiting for parallel process finish.")

        # restructure data folders
        for src in procs:
            _move_proc(src, self.root)
            shutil.rmtree(src)

        with io.open(self.cfgpath, "wb") as fp:
            pickle.dump(self.config, fp)
            fp.flush()
            os.fsync(fp.fileno())

        beginpath = os.path.join(self.root, _BEGIN_FILE)

        if os.path.isfile(beginpath):
            os.remove(beginpath)

        # archive if requested
        if self.config["__control__"]["archive"]:
            slabpath = self.config["__control__"]["slabpath"]
            dirname, basename = os.path.split(self.root)

            with tarfile.open(slabpath, "w") as tar:
                for item in os.listdir(self.root):
                    itempath = os.path.join(self.root, item)
                    tar.add(itempath, arcname=item)

            shutil.rmtree(self.root)

        # TODO: coordinate with slaves removing output paths


class ParallelPyslabsReader():

    def __init__(self, workdir, config):
        self.root = workdir
        self.cfgpath = os.path.join(self.root, _CONFIG_FILE)
        self.config = config
        self.slabpath = config["__control__"]["slabpath"]

    def get_array(self, name, squeeze=False):

        varcfg = self.config["vars"][name]

        var = VariableReader(os.path.join(self.root, name), varcfg)

        return wrap.get_array(var, squeeze)

    def close(self):
        pass

    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        pass


class MasterPyslabsReader(ParallelPyslabsReader):

    def __init__(self, workdir, config):

        super(MasterPyslabsReader, self).__init__(workdir, config)

        with tarfile.open(self.slabpath, "r") as tar:
            tar.extractall(self.root)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.close()

    def close(self):

        if os.path.isdir(self.root):
            shutil.rmtree(self.root)


def master_open(slabpath, mode="r", nprocs=1, archive=True, workdir=None):

    if slabpath.endswith(_EXT) or slabpath.endswith(_CEXT):
        base, ext = os.path.splitext(slabpath)
        beginpath = base + _BEGIN_EXT

        if workdir is None:
            workdir = base + _WORKDIR_EXT
    else:
        beginpath = slabpath + _BEGIN_EXT
        if workdir is None:
            workdir = slabpath + _WORKDIR_EXT
        slabpath += _EXT

    # create root directory
    os.makedirs(workdir, exist_ok=True)
    for item in os.listdir(workdir):
        itempath = os.path.join(workdir, item)

        if os.path.isdir(itempath):
            shutil.rmtree(itempath)
        else:
            os.remove(itempath)

    if mode == "w":

        # create a config file
        with io.open(beginpath, "wb") as fp:
            begin = {"workdir": workdir}
            pickle.dump(begin, fp)
            fp.flush()
            os.fsync(fp.fileno())

        if not os.path.isfile(beginpath):
            raise Exception("Can not create a flag file: %s" % beginpath)

    if not os.path.isdir(workdir):
        raise Exception("Work directory does not exist: %s" % workdir)

    cfg = _CONFIG_INIT
    cfg["__control__"]["nprocs"] = nprocs
    cfg["__control__"]["archive"] = archive
    cfg["__control__"]["beginpath"] = beginpath
    cfg["__control__"]["slabpath"] = slabpath

    if mode[0] == "w":
        return MasterPyslabsWriter(workdir, cfg)

    elif mode[0] == "r":
        return MasterPyslabsReader(workdir, cfg)

    else:
        raise Exception("Unknown open mode: %s" % str(mode))


def parallel_open(slabpath, mode="r"):
 
    if slabpath.endswith(_EXT) or slabpath.endswith(_CEXT):
        base, ext = os.path.splitext(slabpath)
        beginpath = base + _BEGIN_EXT

    else:
        beginpath = slabpath + _BEGIN_EXT

    start = time.time()
    while time.time() - start < _MAX_OPEN_WAIT:
        if os.path.isfile(beginpath):
            with io.open(beginpath, "rb") as fp:
                begin = pickle.load(fp)
                workdir = begin["workdir"]
            break
        time.sleep(0.1)

    if workdir is None:
        raise Exception("No begin notification: %s" % beginpath)
 
    start = time.time()
    while time.time() - start < _MAX_OPEN_WAIT:

        cfgpath = os.path.join(workdir, _CONFIG_FILE)

        if not os.path.isfile(cfgpath):
            time.sleep(0.1)
            continue

        with io.open(cfgpath, "rb") as fp:
            cfg = pickle.load(fp)

            if mode[0] == "w":
                return ParallelPyslabsWriter(workdir, cfg)

            elif mode[0] == "r":
                return ParallelPyslabsReader(workdir, cfg)

            else:
                raise Exception("Unknown open mode: %s" % str(mode))

    raise Exception("Target configuration is not configured: %s" % cfgpath)


def open(*vargs, **kwargs):
    return master_open(*vargs, **kwargs)
