import os, pickle, shutil, time, uuid

from pyslabs import wrap


_CONFIG_FILE = "__config__"
_BEGIN_FILE = "__begin__"
_FINISHED = "__finished__"
_MAX_OPEN_WAIT = 10 # seconds
_MAX_CLOSE_WAIT = 100 # seconds
_CONFIG_INIT = {
    "version": 1,
    "dims": {},
    "vars": {},
    "attrs": {},
    "__control__": {
        "master": None,
        "nprocs": 1,
    }
} 


class VariableWriter():

    def __init__(self, path, config):

        self.path = path
        self.config = config
        self.writecount = 0

    def write(self, slab, start):

        path = self.path

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

    def get_var(self, name):

        varcfg = self.config["vars"][name]

        return VariableWriter(os.path.join(self.path, name), varcfg)

    def close(self):

        # notify master that it is finished
        with open(os.path.join(self.path, _FINISHED), "w") as fp:
            fp.write("DONE")
            fp.flush()
            os.fsync(fp.fileno())


class MasterPyslabsWriter(ParallelPyslabsWriter):

    def begin(self):
 
        self.config["__control__"]["master"] = {self.uuid: None}

        with open(self.cfgpath, "wb") as fp:
            pickle.dump(self.config, fp)
            fp.flush()
            os.fsync(fp.fileno())

    def define_var(self, name):

        varcfg = {}
        self.config["vars"][name] = varcfg

        return VariableWriter(os.path.join(self.path, name), varcfg)

    def close(self):
 
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

                except ValueError:
                    pass

            if len(procs) == nprocs:
                break

        if len(procs) != nprocs:
            raise Exception("Number of processes mismatch: %d != %d" %
                    (len(procs), nprocs))

        # restructure data folders
        for src in procs:
            _move_proc(src, self.root)
            shutil.rmtree(src)

        self.config["__control__"]["master"] = None

        with open(self.cfgpath, "wb") as fp:
            pickle.dump(self.config, fp)
            fp.flush()
            os.fsync(fp.fileno())
#
#        # archive if requested
#        if self.archive:
#            dirname, basename = os.path.split(self.root)
#            arcpath = os.path.join(dirname, basename+_EXT)
#
#            with tarfile.open(arcpath, "w") as tar:
#                tar.add(self.root, arcname=basename)
#
#            #shutil.rmtree(self.root)

        # TODO: coordinate with slaves removing output paths


class ParallelPyslabsReader():

    def __init__(self, root, config):
        self.root = root
        self.cfgpath = os.path.join(self.root, _CONFIG_FILE)
        self.config = config

    def get_array(self, name):

        varcfg = self.config["vars"][name]

        var = VariableReader(os.path.join(self.root, name), varcfg)

        return wrap.get_array(var)

    def close(self):

        pass

    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        pass


class MasterPyslabsReader(ParallelPyslabsReader):
    pass


def master_open(path, mode="r", nprocs=1):

    beginpath = os.path.join(path, _BEGIN_FILE)

    if mode == "w":

        # create root directory
        os.makedirs(path, exist_ok=False)

        # create a config file
        with open(beginpath, "wb") as fp:
            pickle.dump(nprocs, fp)
            fp.flush()
            os.fsync(fp.fileno())

        if not os.path.isfile(beginpath):
            raise Exception("Can not create a flag file: %s" % beginpath)

    if not os.path.isdir(path):
        raise Exception("Target path does not exist: %s" % path)

    cfg = _CONFIG_INIT
    cfg["__control__"]["nprocs"] = nprocs

    if mode[0] == "w":
        return MasterPyslabsWriter(path, cfg)

    elif mode[0] == "r":
        return MasterPyslabsReader(path, cfg)

    else:
        raise Exception("Unknown open mode: %s" % str(mode))


def parallel_open(path, mode="r"):

    start = time.time()
    while time.time() - start < _MAX_OPEN_WAIT:
        if os.path.isdir(path):
            break
        time.sleep(0.1)

    if not os.path.isdir(path):
        raise Exception("Target path does not exist: %s" % path)
 
    beginpath = os.path.join(path, _BEGIN_FILE)

    start = time.time()
    while time.time() - start < _MAX_OPEN_WAIT:
        if os.path.isfile(beginpath):
            break
        time.sleep(0.1)

    if not os.path.isfile(beginpath):
        raise Exception("No begin notification: %s" % beginpath)
 
    start = time.time()
    while time.time() - start < _MAX_OPEN_WAIT:

        cfgpath = os.path.join(path, _CONFIG_FILE)

        if not os.path.isfile(cfgpath):
            time.sleep(0.1)
            continue

        with open(cfgpath, "rb") as fp:
            cfg = pickle.load(fp)

            if mode[0] == "w":
                return ParallelPyslabsWriter(path, cfg)

            elif mode[0] == "r":
                return ParallelPyslabsReader(path, cfg)

            else:
                raise Exception("Unknown open mode: %s" % str(mode))

    raise Exception("Target configuration is not configured: %s" % cfgpath)
