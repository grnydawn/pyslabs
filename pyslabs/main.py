import os, io, pickle, shutil, time, uuid, tarfile, copy, itertools, pprint
from collections import OrderedDict
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

        elif isinstance(start, int):
            start = (start,) + (0,) * (len(slabshape) - 1)

        else:
            start = start + (0,) * (len(slabshape) - len(start))

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

        for _s in start:
            slabpath.append(str(_s))

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


# TODO: add slab-level services
# TODO: add slice to array
# TODO: use buffer for tarfile

class VariableReader():

    def __init__(self, tfile, slabtower, config, start=None, shape=None):

        self._tfile = tfile
        self._slabtower = slabtower
        self._config = config
        self.shape = shape if shape else tuple(self._config["shape"])
        #self._start = start if start else tuple([0]*len(self.shape))

    @property
    def ndim(self):
        return len(self.shape)

    def __len__(self):
        return self.shape[0]

    # TODO: generate a list of indexes and find out start stop

    def _get_slice(self, k, length):

        if isinstance(k, int):
            if k < 0:
                start, stop, step = length+k, length+k+1, 1

            else:
                start, stop, step = k, k+1, 1

        else:
            start, stop, step = k.start or 0, k.stop or length, k.step or 1

        minval = length
        maxval = 0

        for i in range(start, stop, step):
            if i < minval:
                minval = i

            if i > maxval:
                maxval = i

        return slice(minval, maxval + 1, step)

    def _merge_stack(self, tower, tkey, shape, newkey):

        _k = self._get_slice(tkey, shape[0])

        _m = []
        atype = None

        for name, tinfo in itertools.islice(tower.items(),
                _k.start, _k.stop, _k.step):

            _, _atype, _ = name.split(".")

            if atype is None:
                atype = _atype

            elif atype != _atype:
                raise Exception("Array type mismatch: %s != %s" % (str(atype), str(_atype)))

            _, slab = data.load(self._tfile, tinfo, atype)

            slab = data.get_slice(slab, atype, newkey)

            _m.append(slab)

        _a = data.stack(_m, atype)
        #if len(_a) == 1:
        #    _a = _a[0]

        return (atype, _a)

    def _get_array(self, tower, key, tkey, shape, newkey=None):

        # create a new list for processing at this level
        newkey = list() if newkey is None else list(newkey)

        if len(key) == 0:
            return self._merge_stack(tower, tkey, shape, newkey)

        _k = self._get_slice(key[0], shape[0])

        _m = []

        # next indices
        nidxes = list(tower.keys())[1:] + [shape[0]]

        tlen = 0 # total length
        slen = 0 # slice length
        atype = None

        for (idx, val), nidx in zip(tower.items(), nidxes):

            idx = int(idx)
            nidx = int(nidx)

            if nidx <= _k.start:
                tlen += nidx - idx
                continue

            if idx >= _k.stop:
                break

            if slen == 0:
                a = _k.start - tlen
                slen = nidx - _k.start

            else:
                a = (_k.step - slen % _k.step) % _k.step
                slen += nidx - idx

            if nidx >= _k.stop:
                b = _k.stop - tlen

            else:
                b = nidx - idx

            #newkey.append(slice(a, b, _k.step))
            _nk = newkey + [slice(a, b, _k.step)]

            #_atype, _o = self._get_array(val, key[1:], tkey, shape[1:], newkey)
            _atype, _o = self._get_array(val, key[1:], tkey, shape[1:], _nk)

            if _o:
                if atype is None:
                    atype = _atype

                elif atype != _atype:
                    raise Exception("Array type mismatch: %s != %s" % (str(atype), str(_atype)))

                _m.append((atype, _o))

            tlen += nidx - idx

        _x = [None, None]

        for _i in _m:
            data._concat(_x, _i)

        return _x

    def __getitem__(self, key):

        ndim = self.ndim

        if isinstance(key, int):
            key = (key,) + (slice(None, None, None),) * (ndim-1)

        elif len(key) < ndim:
            key = key + (slice(None, None, None),) * (ndim-len(key))

        # put stack dimension at the last
        shape = list(self.shape)
        shape = shape[1:] + [shape[0]]

        atype, array = self._get_array(self._slabtower, key[1:], key[0], shape)

        if array and data.length(array, 0) == 1:
            array = data.squeeze(array, atype)

        return array

    def __iter__(self):
        import pdb; pdb.set_trace()

    def __next__(self):
        import pdb; pdb.set_trace()

    def __contains__(self, item):
        import pdb; pdb.set_trace()


class PyslabsWriter():

    def __init__(self, root, config):
        self.root = root
        self.uuid = str(uuid.uuid4().hex)
        self.path = os.path.join(self.root, self.uuid)
        self.cfgpath = os.path.join(self.root, _CONFIG_FILE)
        self.config = config

        os.makedirs(self.path)

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


class ParallelPyslabsWriter(PyslabsWriter):

    def get_writer(self, name):

        varcfg = self.config["vars"][name]

        return VariableWriter(os.path.join(self.path, name), varcfg)


class MasterPyslabsWriter(PyslabsWriter):

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

    def get_writer(self, name, shape=None):

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

        _shape = {}

        for vn, vc in attrs["vars"].items():
            _sp = None

            for _vcfg in vc["config"]:
                _vshape = _vcfg["shape"]
                if _sp is None:
                    _sp = _vshape

                elif _sp[0] != _vshape[0] or _sp[2:] != _vshape[2:]:
                    raise Exception("Shape mismatch: %s != %s" % (str(_sp), str(_vshape)))

                else:
                    _sp[1] += _vshape[1]

            _shape[vn] = _sp


        for name, varcfg in self.config["vars"].items():

            varcfg["shape"] = _shape[name]
            varcfg.pop("writes")

            if varcfg["check"]:
                for check, test in varcfg["check"].items():
                    if check == "shape":
                        if isinstance(test, int):
                            if _shape[name][0] != test:
                                raise Exception("stack dimension mismatch: %d != %d" %
                                        (_shape[name][0], test))

                        elif len(test) > 0:

                            if test[0] is not True and _shape[name][0] != test[0]:
                                raise Exception("stack dimension mismatch: %d != %d" %
                                        (_shape[name][0], test[0]))

                            if tuple(test[1:]) != tuple(_shape[name][1:]):
                                raise Exception("slab shape mismatch: %s != %s" %
                                        (str(test[1:]), str(tuple(_shape[name][1:]))))

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


class ParallelPyslabsReader():

    def __init__(self, slabpath):
        self.slabpath = slabpath
        self.slabarchive = tarfile.open(slabpath)
        self.slabtower = OrderedDict()

        _tower = {}

        for entry in self.slabarchive:
            if entry.name == _CONFIG_FILE:
                self.config = pickle.load(self.slabarchive.extractfile(entry))
            else:
                self._trie(_tower, entry.path.split("/"), entry)

        self._sort_tower(self.slabtower, _tower)

    def _sort_tower(self, st, t):

        for k in sorted(t.keys()):
            v = t[k]

            if isinstance(v, dict):
                _st = OrderedDict()
                st[k] = _st
                self._sort_tower(_st, v)

            else:
                st[k] = v

    def _trie(self, tower, path, entry):

        if len(path) == 1:

            if isinstance(tower, tarfile.TarInfo):
                import pdb; pdb.set_trace()

            elif path[0] in tower:
                raise Exception("Wrong mapping: %s" % path)

            tower[path[0]] = entry

        elif path[0] in tower:

            if isinstance(tower[path[0]], tarfile.TarInfo):
                tower[path[0]] = {}
                self._trie(tower[path[0]], path[1:], entry)

            else:
                self._trie(tower[path[0]], path[1:], entry)
        else:
            _t = {}
            tower[path[0]] = _t
            self._trie(_t, path[1:], entry)

    def __del__(self):
        self.slabarchive.close()

    def get_reader(self, name, squeeze=False):

        varcfg = self.config["vars"][name]

        return VariableReader(self.slabarchive, self.slabtower[name], varcfg)

    def get_array(self, name, squeeze=False):

        return data.get_array(self.slabarchive, self.slabtower[name], squeeze)

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
