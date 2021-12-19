import os, io, pickle, shutil, time, uuid, tarfile, copy

from pyslabs import data


_CONFIG_FILE = "__config__"
_BEGIN_FILE = "__begin__"
_VARCFG_FILE = "__varcfg__"
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
_VARCFG_INIT = {
    "writes": {},
    "shape": None,
    "check": {}
} 

class VariableWriter():

    def __init__(self, path, config):

        self.path = path
        self.config = config
        self.writecount = 0

    def write(self, slab, start=None, shape=None):

        # get slab info
        slabshape = data.shape(slab)
        slabndim = len(slabshape)

        # shape check
        if shape:
            if tuple(shape) != slabshape:
                raise Exception("Shape check fails: %s != %s" %
                        (str(shape) != str(slabshape)))

        if start is None:
            start = (0,) * len(slabshape)

        # generate shape
        if self.config["shape"] is None:
            self.config["shape"] = [1] + list(slabshape)

        else:
            if tuple(self.config["shape"][1:]) != tuple(slabshape):
                raise Exception("Shape check fails: %s != %s" %
                        (str(self.config["shape"][1:]) != str(slabshape)))

            self.config["shape"][0] += 1

        # generate relative path to data file

        slabpath = []

        try:
            for _s in start:
                slabpath.append(str(_s))

        except TypeError:
            slabpath.append(str(start))

        wc = str(self.writecount)
        
        if wc in self.config["writes"]:
            writes = self.config["writes"][wc]

        else:
            writes = {}
            self.config["writes"][wc] = writes

        writes["/".join(slabpath)] = (start, slabshape)

        path = os.path.join(self.path, *slabpath)

        if not os.path.isdir(path):
            os.makedirs(path)

        atype, ext = data.arraytype(slab)
        slabpath = os.path.join(path, ".".join([wc, atype, ext])) 

        data.dump(slab, slabpath)

        self.writecount += 1


class VariableReader():

    def __init__(self, tfile, slabmap, config):

        self._tfile = tfile
        self._slabmap = slabmap
        self._config = config
        self.shape = tuple(self._config["shape"])

    @property
    def ndim(self):
        return len(self.shape)

    def __len__(self):
        import pdb; pdb.set_trace()

    def __getitem__(self, key):
        import pdb; pdb.set_trace()

    def __setitem__(self, key, value):
        import pdb; pdb.set_trace()

    def __delitem__(self, key):
        import pdb; pdb.set_trace()

    def __missing__(self, key):
        import pdb; pdb.set_trace()

    def __iter__(self):
        import pdb; pdb.set_trace()

    def __next__(self):
        import pdb; pdb.set_trace()

    def __reversed__(self):
        import pdb; pdb.set_trace()

    def __contains__(self, item):
        import pdb; pdb.set_trace()


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

        for name, cfg in self.config["vars"].items():
            with io.open(os.path.join(self.path, name, _VARCFG_FILE), "wb") as fp:
                pickle.dump(cfg, fp)
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

    def define_var(self, name, shape=None):

        varcfg = copy.deepcopy(_VARCFG_INIT)

        if shape:
            varcfg["check"]["shape"] = shape

        self.config["vars"][name] = varcfg

        return VariableWriter(os.path.join(self.path, name), varcfg)

    def close(self):
 
        super(MasterPyslabsWriter, self).close()

        beginpath = self.config["__control__"]["beginpath"]

        if os.path.isfile(beginpath):
            os.remove(beginpath)

        def _move_dim(src, dst, attrs):
            
            for dim in os.listdir(src):
                srcpath = os.path.join(src, dim)
                dstpath = os.path.join(dst, dim)

                if os.path.isdir(srcpath):
                    if os.path.isdir(dstpath):
                        _move_dim(srcpath, dstpath, attrs) 

                    elif os.path.exists(dstpath):
                        raise Exception("Destination path already exists: %s" % dstpath)

                    else:
                        shutil.move(srcpath, dstpath)

                elif os.path.exists(dstpath):
                    raise Exception("Multiple processes creat the same data file: %s" % dstpath)

                else:
                    shutil.move(srcpath, dstpath)
              
        def _move_proc(src, dst, attrs):

            for var in os.listdir(src): 

                dstvar = os.path.join(dst, var)
                srcvar = os.path.join(src, var)

                if not var.startswith("_") and var not in attrs["vars"]:
                    attrs["vars"][var] = {"config": []}

                varcfg = os.path.join(srcvar, _VARCFG_FILE)

                with io.open(varcfg, "rb") as fp:
                    _cfg = pickle.load(fp)

                attrs["vars"][var]["config"].append(_cfg)

                os.remove(varcfg)

                if os.path.isdir(dstvar):
                    _move_dim(srcvar, dstvar, attrs) 

                else:
                    shutil.move(srcvar, dstvar)

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

        attrs = {"vars": {}}

        # restructure data folders
        for src in procs:
            _move_proc(src, self.root, attrs)
            shutil.rmtree(src)

        _shape = None

        for vn, vc in attrs["vars"].items():
            for _vcfg in vc["config"]:
                _vshape = _vcfg["shape"]
                if _shape is None:
                    _shape = _vshape

                elif _shape[0] != _vshape[0] or _shape[2:] != _vshape[2:]:
                    raise Exception("Shape mismatch: %s != %s" % (str(_shape), str(_vshape)))

                else:
                    _shape[1] += _vshape[1]

        for name, varcfg in self.config["vars"].items():

            if varcfg["check"]:
                for check, test in varcfg["check"].items():
                    if check == "shape":
                        if isinstance(test, int):
                            if _shape[0] != test:
                                raise Exception("stack dimension mismatch: %d != %d" %
                                        (_shape[0], test))

                        elif len(test) > 0:

                            if test[0] is not True and _shape[0] != test[0]:
                                raise Exception("stack dimension mismatch: %d != %d" %
                                        (_shape[0], test[0]))

                            if tuple(test[1:]) != tuple(_shape[1:]):
                                raise Exception("slab shape mismatch: %s != %s" %
                                        (str(test[1:]), str(tuple(_shape[1:]))))

                    else:
                        raise Exception("Unknown variable test: %s" % check)


        archive = self.config["__control__"]["archive"]
        slabpath = self.config["__control__"]["slabpath"]

        self.config.pop("__control__")

        with io.open(self.cfgpath, "wb") as fp:
            pickle.dump(self.config, fp)
            fp.flush()
            os.fsync(fp.fileno())

        if os.path.isfile(beginpath):
            os.remove(beginpath)

        # archive if requested
        if archive:
            dirname, basename = os.path.split(self.root)

            with tarfile.open(slabpath, "w") as tar:
                for item in os.listdir(self.root):
                    itempath = os.path.join(self.root, item)
                    tar.add(itempath, arcname=item)

            shutil.rmtree(self.root)

        # TODO: coordinate with slaves removing output paths


# TODO: do not create workdir just use tarfile object

class ParallelPyslabsReader():

    def __init__(self, slabpath):
        self.slabpath = slabpath
        self.slabarc = tarfile.open(slabpath)
        self.slabmap = {}

        for entry in self.slabarc:
            if entry.name == _CONFIG_FILE:
                self.config = pickle.load(self.slabarc.extractfile(entry))

            self._trie(self.slabmap, entry.path.split("/"), entry)

    def _trie(self, pmap, path, entry):

        if len(path) == 1:

            if path[0] in pmap:
                raise Exception("Wrong mapping: %s" % path)

            pmap[path[0]] = entry

        elif path[0] in pmap and isinstance(pmap[path[0]], dict):
            self._trie(pmap[path[0]], path[1:], entry)

        else:
            newmap = {}
            pmap[path[0]] = newmap
            self._trie(newmap, path[1:], entry)

    def __del__(self):
        self.slabarc.close()

    def get_var(self, name, squeeze=False):

        varcfg = self.config["vars"][name]

        return VariableReader(self.slabarc, self.slabmap[name], varcfg)

    def get_array(self, name, squeeze=False):

        return data.get_array(self.slabarc, self.slabmap[name], squeeze)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass


class MasterPyslabsReader(ParallelPyslabsReader):

    def __init__(self, slabpath):

        super(MasterPyslabsReader, self).__init__(slabpath)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.close()


def master_open(slabpath, mode="r", nprocs=1, archive=True, workdir=None):
 
    if mode == "w":
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

        with io.open(beginpath, "wb") as fp:
            begin = {"workdir": workdir}
            pickle.dump(begin, fp)
            fp.flush()
            os.fsync(fp.fileno())

        if not os.path.isfile(beginpath):
            raise Exception("Can not create a flag file: %s" % beginpath)

        if not os.path.isdir(workdir):
            raise Exception("Work directory does not exist: %s" % workdir)

        cfg = copy.deepcopy(_CONFIG_INIT)
        cfg["__control__"]["nprocs"] = nprocs
        cfg["__control__"]["archive"] = archive
        cfg["__control__"]["beginpath"] = beginpath
        cfg["__control__"]["slabpath"] = slabpath

        return MasterPyslabsWriter(workdir, cfg)

    elif mode[0] == "r":

        return MasterPyslabsReader(slabpath)

    else:
        raise Exception("Unknown open mode: %s" % str(mode))


def parallel_open(slabpath, mode="r"):
 
    if mode == "w":
        base, ext = os.path.splitext(slabpath)
        beginpath = (base if ext else slabpath) + _BEGIN_EXT

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

            return ParallelPyslabsWriter(workdir, cfg)

    elif mode[0] == "r":
        return ParallelPyslabsReader(slabpath)

    else:
        raise Exception("Unknown open mode: %s" % str(mode))

    raise Exception("Target configuration is not configured: %s" % cfgpath)


def open(*vargs, **kwargs):
    return master_open(*vargs, **kwargs)
