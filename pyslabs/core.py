"""Pyslabs core module


naming conventions
======================

path : may includes directory path
name : does not include directory path

"""

import os, sys, io, copy, time, uuid, pickle, shutil, tarfile

from collections import OrderedDict
from pyslabs.const import (SLAB_EXT, ZLAB_EXT, TMP_BEGIN, TMP_WORK, INIT_BEGIN,
                           INIT_CONFIG, INIT_VARCFG, INIT_DIMCFG, CONFIG_FILE,
                           FINISH_FILE, INIT_TIMEOUT, FINI_TIMEOUT, VARCFG_FILE,
                           UNLIMITED)
from pyslabs.error import PE_Begin_Numproc, PE_Close_Startindexerror, PE_Close_Shapemismatch
from pyslabs.util import pickle_dump, clean_folder
from pyslabs.write import VariableWriterV1
from pyslabs.read import VariableReaderV1
from pyslabs.slabif import _cache


##############################
# PRIVATE FUNCTIONS
##############################

def _write_paths(slab_path, work_path):

    if slab_path.endswith(SLAB_EXT) or slab_path.endswith(ZLAB_EXT):
        base, ext = os.path.splitext(slab_path)

        if work_path is None:
            begin_path = base + TMP_BEGIN
            work_path = base + TMP_WORK

        else:
            begin_path = os.path.join(work_path, TMP_BEGIN)

    else:

        if work_path is None:
            begin_path = slab_path + TMP_BEGIN
            work_path = base + TMP_WORK

        else:
            begin_path = os.path.join(work_path, TMP_BEGIN)

        slab_path += SLAB_EXT

#
#    if slab_path.endswith(SLAB_EXT) or slab_path.endswith(ZLAB_EXT):
#        base, ext = os.path.splitext(slab_path)
#
#        if work_path is None:
#            begin_path = base + TMP_BEGIN
#            work_path = base + TMP_WORK
#
#        else:
#            begin_path = work_path + TMP_BEGIN
#
#    else:
#
#        if work_path is None:
#            begin_path = slab_path + TMP_BEGIN
#            work_path = base + TMP_WORK
#
#        else:
#            begin_path = work_path + TMP_BEGIN
#
#        slab_path += SLAB_EXT

    return slab_path, begin_path, work_path


##############################
# PUBLIC CLASSES
##############################

class Dimension():

    def __init__(self, config):
        self.config = config

    def __getattr__(self, name):
        if name in self.config:
            return self.config[name]

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)


    def check(self, length):

        mylen = self.config["length"]

        if mylen is None:
            return True

        if length <  0 or (mylen != UNLIMITED and mylen != length):
            return False

        return True

class StackDimension(Dimension):
    pass


# implement slab structure and protocol
class PyslabsWriterV1(object):

    def __init__(self, work_path, config):
        self.work_path = work_path
        self.uuid = str(uuid.uuid4().hex)
        self.proc_path = os.path.join(work_path, self.uuid)
        self.cfg_path = os.path.join(work_path, CONFIG_FILE)
        self.config = config

        os.makedirs(self.proc_path)

    def close(self):

        for name, cfg in self.config["vars"].items():
            cfg_path = os.path.join(self.proc_path, name, VARCFG_FILE)
            pickle_dump(cfg_path, cfg)

        finish_path = os.path.join(self.proc_path, FINISH_FILE)

        with io.open(finish_path, "w") as fp:
            fp.write("FINISHED")
            fp.flush()
            os.fsync(fp.fileno())


# master implementation of pyslabs 
class MasterPyslabsWriterV1(PyslabsWriterV1):

    def get_writer(self, name, shape=None, autostack=False, **kwargs):

        var_cfg = copy.deepcopy(INIT_VARCFG)

        if shape is not None:
            sh = []
            for i, l in enumerate(shape):
                if isinstance(l, Dimension):
                    sh.append(l)

                elif isinstance(l, int):
                    if i == 0:
                        self.define_stack("stack", l)
                    else:
                        self.define_dim("dim%d"%i, l)
 
                elif l is None:
                    if i == 0:
                        self.define_stack("stack", UNLIMITED)
                    else:
                        self.define_dim("dim%d"%i, UNLIMITED)
               
                else:
                    raise PE_Core_Invalidshapetype(str(shape))

            shape = tuple(shape)

        var_cfg["check"]["shape"] = shape
        var_cfg["stack"]["auto"] = autostack
        var_cfg["attrs"].update(dict((k[5:],v) for k,v in kwargs.items() if
                                k.startswith("attr_")))

        self.config["vars"][name] = var_cfg

        return VariableWriterV1(os.path.join(self.proc_path, name), var_cfg)

    def define_dim(self, name, length, origin=(0, "O"), unit=(1, ""),
                    points=None, desc="N/A", **kwargs):

        if isinstance(origin, int):
            origin = (origin, "O")

        if isinstance(unit, int):
            unit = (unit, "")

        if points is not None:
            _length = len(points)
            if length is not None and _length != length:
                raise Exception("Dimension '%s' length mismatch: %d != %d" %
                                (name, length, _length))

            origin = (points[0], origin[1])
            unit = (None, unit[1])

        dim_cfg = copy.deepcopy(INIT_DIMCFG)
        dim_cfg["name"] = name
        dim_cfg["length"] = length
        dim_cfg["origin"] = origin
        dim_cfg["unit"] = unit
        dim_cfg["points"] = points
        dim_cfg["desc"] = desc
        dim_cfg["attrs"] = dict((k[5:],v) for k,v in kwargs.items() if
                                k.startswith("attr_"))

        self.config["dims"][name] = dim_cfg

        return Dimension(dim_cfg)

    def define_stack(self, name, length, origin=(0, "O"), unit=(1, "slab"),
                    points=None, desc="N/A", **kwargs):

        dim = self.define_dim(name, length, origin=origin, unit=unit,
                                points=points, desc=desc, **kwargs)

        if dim.config["unit"]:
            if (not isinstance(dim.config["unit"][0], int) and
                dim.config["unit"][0] < 0):
                raise Exception("Stack unit is not a positive integer: %s" %
                    str(dim.config["unit"][0]))

        return StackDimension(dim.config)

    def begin(self):

        pickle_dump(self.cfg_path, self.config)

        procs = []

        start = time.time()
        num_procs = self.config["_control_"]["num_procs"]

        while time.time() - start < INIT_TIMEOUT:

            procs.clear()

            for item in os.listdir(self.work_path):
                if item == self.uuid:
                    procs.append(os.path.join(self.work_path, item))
                    time.sleep(0.1)
                    continue

                try:
                    if len(item) == len(self.uuid) and int(item, 16):
                        proc = os.path.join(self.work_path, item)
                        procs.append(proc)

                except ValueError:
                    pass

            if len(procs) == num_procs:
                break

        if len(procs) != num_procs:
            raise PE_Begin_Numproc("%d != %d" %(len(procs), num_procs))

    def close(self):

        super(MasterPyslabsWriterV1, self).close()

        begin_path = self.config["_control_"]["begin_path"]

        if os.path.isfile(begin_path):
            os.remove(begin_path)

        # dim: dimension to scan, start indices of the dimension, slab_shape
        # TODO : get shape info from var config of each procs
        #def _scan(dim, start, slab_shape):
        def _scan(dim, start_length):

            st = None
            sh = None
            prev_st_len = None

            # sorted start indices
            for st_len in sorted(start_length.keys(), key=lambda x:x[0]):

                # indices of next dimension
                next_dim = start_length[st_len]

                # if stack dimension
                if next_dim is None:

                    # return start index and # of slabs
                    return [st_len[0]], [st_len[1]]

                else:
                    # if non-stack dimension, go to next dimension
                    _st, _sh = _scan(dim+1, next_dim)
                    # return with start and shape

                    # if the first index
                    if st is None:
                        st = [st_len[0]] + _st
                    elif sum(prev_st_len) != sh[0]:
                        raise PE_Close_Startindexerror("%s != %d" % (prev_st_len, sh[0]))

                    if sh is None:
                        sh = [st_len[1]] + _sh
                    else:
                        sh[0] = sum(st_len)

                prev_st_len = st_len

            return st, sh

        def _move_dim(src, dst, start_length):

            nslabs = None

            for idx_len in os.listdir(src):

                src_path = os.path.join(src, idx_len)
                dst_path = os.path.join(dst, idx_len)

                if os.path.isdir(src_path):

                    st_len = tuple(int(i) for i in idx_len.split("_"))

                    if st_len not in start_length:
                        dim_st_len = {}
                        start_length[st_len] = dim_st_len

                    else:
                        dim_st_len = start_length[st_len]

                    if os.path.isdir(dst_path):
                        _move_dim(src_path, dst_path, dim_st_len)

                    elif os.path.exists(dst_path):
                        raise PE_Close_DestExist(dst_path)

                    else:
                        os.makedirs(dst_path)
                        _move_dim(src_path, dst_path, dim_st_len)
                        #shutil.move(src_path, dst_path)

                elif os.path.exists(dst_path):
                    raise PE_Close_DestDupulicated(dst_path)

                else:
                    nslabs = 1 if nslabs is None else nslabs + 1
                    shutil.move(src_path, dst_path)

            if nslabs is not None:
                start_length[(0, nslabs)] = None

        def _move_proc(src, dst, attrs):

            for var in os.listdir(src):

                dst_path = os.path.join(dst, var)
                src_path = os.path.join(src, var)

                if not var.startswith("_") and var not in attrs["vars"]:
                    start_length = {}
                    attrs["vars"][var] = {"config": [], "start_length":start_length}

                else:
                    start_length = attrs["vars"][var]["start_length"]

                cfg_path = os.path.join(src_path, VARCFG_FILE)

                with io.open(cfg_path, "rb") as fp:
                    cfg = pickle.load(fp)
                    attrs["vars"][var]["config"].append(cfg)

                os.remove(cfg_path)

                if not os.path.isdir(dst_path):
                    os.makedirs(dst_path)

                _move_dim(src_path, dst_path, start_length)

#        def _get_shape(writes):
#
#            height = 0
#            shape = None
#
#            for stack, write in writes.items():
#                _shape = [{}]*len(ndim)
#                for start, slab_shape in sorted(write.values(), key=lambda x:x[0]):
#                    _shape_merge(_shape, start, slab_shape)
#                    import pdb; pdb.set_trace()
#
#                if shape is None:
#                    shape = _shape
#
#                elif shape != _shape:
#                    raise PE_Close_Shapemismatch("%s != %s" %
#                            (str(shape), str(_shape)))
#
#                height += 1 
#
#            import pdb; pdb.set_trace()
#            return [height] + shape

        procs = []

        start_time = time.time()
        num_procs = self.config["_control_"]["num_procs"]

        while time.time() - start_time < FINI_TIMEOUT:

            procs.clear()

            for item in os.listdir(self.work_path):
                if item == self.uuid:
                    procs.append(os.path.join(self.work_path, item))
                    time.sleep(0.1)
                    continue

                try:
                    if len(item) == len(self.uuid) and int(item, 16):
                        proc = os.path.join(self.work_path, item)
                        procs.append(proc)

                except ValueError:
                    pass

            if len(procs) == num_procs:
                break

        if len(procs) != num_procs:
            raise PE_Close_Numproc("%d != %d" %(len(procs), num_procs))

        for proc in procs:
            finish_path = os.path.join(proc, FINISH_FILE)
            timeout = True

            while time.time() - start_time < FINI_TIMEOUT:
                if os.path.isfile(finish_path):
                    os.remove(finish_path)
                    timeout = False
                    break
                time.sleep(0.1)

            if timeout:
                raise PE_Close_Timeout()

        # Now, it is true that all parallel writes are fininished.

        attrs = {"vars": {}}

        # restructure data folders
        for proc_path in procs:
            _move_proc(proc_path, self.work_path, attrs)
            shutil.rmtree(proc_path)

        start = {}
        shape = {}

        # merge shape from each proc
        for var_name, var_info in attrs["vars"].items():
            start[var_name], shape[var_name] = _scan(0, var_info["start_length"])
            shape[var_name] = [shape[var_name][-1]] + shape[var_name][:-1]

            if var_info["config"][0]["check"]["shape"] is None:
                continue

            for idx, sh in enumerate(var_info["config"][0]["check"]["shape"]):

                if hasattr(sh, "name"):
                    dim_cfg = self.config["dims"][sh.name]

                    if dim_cfg["length"] is None or dim_cfg["length"] == UNLIMITED:
                        dim_cfg["length"] = shape[var_name][idx]

                    elif dim_cfg["length"] != shape[var_name][idx]:
                        raise PE_Close_Stackdimmismatch("%d != %d" %
                            (dim_cfg["length"], shape[var_name][idx]))

        for name, var_cfg in self.config["vars"].items():

            var_cfg["shape"] = shape[name]
            var_cfg.pop("writes")

            if var_cfg["check"]:
                # check exists if shape arg is given
                for check in var_cfg["check"].keys(): 

                    dim_checks = var_cfg["check"][check]
                    if dim_checks is None:
                        continue

                    if check == "shape":
                        if isinstance(dim_checks, StackDimension):
                            dim_checks.check(shape[name][0])
                            var_cfg["shape"] = test.name

                        elif len(dim_checks) > 0:

                            if isinstance(dim_checks[0], Dimension):
                                dim_checks[0].check(shape[name][0])
                                var_cfg["shape"][0] = dim_checks[0].name
                            elif (dim_checks[0] is not None and dim_checks[0] !=
                                UNLIMITED and shape[name][0] != dim_checks[0]):
                                raise PE_Close_Shapemismatch("%d != %d" %
                                        (shape[name][0], dim_checks[0]))

                            for i, (dim_check, length) in enumerate(
                                    zip(dim_checks[1:], shape[name][1:])):

                                if isinstance(dim_check, Dimension):
                                    dim_check.check(length)
                                    var_cfg["shape"][i+1] = dim_check.name
                                elif (dim_check is not None and dim_check !=
                                    UNLIMITED and shape[name][i+1] != dim_check):
                                    raise PE_Close_Shapemismatch("%d != %d" %
                                            (shape[name][i+1], dim_check))

                    else:
                        raise PE_Close_Unknowncheck(check)

                var_cfg.pop("check")

        slab_path = self.config["_control_"]["slab_path"]

        self.config.pop("_control_")

        pickle_dump(self.cfg_path, self.config)

        with tarfile.open(slab_path, "w") as tar:
            for item in os.listdir(self.work_path):
                item_path = os.path.join(self.work_path, item)
                tar.add(item_path, arcname=item)

        shutil.rmtree(self.work_path)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.close()


# non-master implementation of pyslabs 
class ParallelPyslabsWriterV1(PyslabsWriterV1):

    def get_writer(self, name, *vargs, **kwargs):

        var_cfg = self.config["vars"][name]

        if "autostack" in kwargs:
            var_cfg["stack"]["auto"] = kwargs["autostack"]

        return VariableWriterV1(os.path.join(self.proc_path, name), var_cfg)

    def get_dim(self, name):

        dim_cfg = self.config["dims"][name]

        return Dimension(dim_cfg)

    def get_stack(self, name):

        dim_cfg = self.config["dims"][name]

        return StackDimension(dim_cfg)

    def begin(self):
        pass


class PyslabsReaderV1():

    def __init__(self, slab_path):
        self.slab_path = slab_path
        self.tar_file = tarfile.open(slab_path, mode="r:")
        self.slab_tower = OrderedDict()

        tower = {}

        for entry in self.tar_file:
            if entry.name == CONFIG_FILE:
                self.config = pickle.load(self.tar_file.extractfile(entry))

            else:
                self._trie(tower, entry.path.split("/"), entry)

        self._sort_tower(self.slab_tower, tower)


    def _sort_tower(self, dst, src):

        for key in sorted(src.keys()):
            value = src[key]

            if isinstance(value, dict):
                _dst = OrderedDict()
                dst[key] = _dst
                self._sort_tower(_dst, value)

            else:
                dst[key] = value

    def _trie(self, output, entry_path, entry):

        if len(entry_path) == 1:

            if (isinstance(output, tarfile.TarInfo) or
                entry_path[0] in output):
                raise PE_Read_Constructtrie(str(entry_path))

            output[entry_path[0]] = entry

        elif entry_path[0] in output:

            if isinstance(output[entry_path[0]], tarfile.TarInfo):
                _output = {}
                output[entry_path[0]] = _output
                self._trie(_output, entry_path[1:], entry)

            else:
                self._trie(output[entry_path[0]], entry_path[1:], entry)
        else:
            _output = {}
            output[entry_path[0]] = _output
            self._trie(_output, entry_path[1:], entry)

    def get_reader(self, name):

        varcfg = self.config["vars"][name]
        dimcfg = self.config["dims"]

        return VariableReaderV1(self.tar_file, self.slab_tower[name],
                varcfg, dimcfg)

    def get_array(self, name, stack=None):

        if stack is None:
            return self.get_reader(name)[:]

        elif isinstance(stack, (int, slice)):
            return self.get_reader(name).__getitem__(stack)

        else:
            return self.get_reader(name).__getitem__(*stack)


    def close(self):

        if not self.tar_file.closed:
            self.tar_file.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _traverse(self, tree, bag):
            try:
                for k, v in tree.items():
                    if bag["check"](k, v):
                        bag["data"].append(bag["output"](k, v))
                    self._traverse(v, bag)

            except AttributeError as err:
                pass

            except Exception as err:
                import pdb; pdb.set_trace()
                print(err)



    def info(self, mode, *args, **kwargs):

        if mode == "list":
            return tuple(self.slab_tower.keys())

        elif mode == "var":
            out = {}
            var = self.get_reader(args[0])

            import pdb; pdb.set_trace()

        elif mode == "slab":
            out = {}
            bag = {"check": lambda k, v: isinstance(v, tarfile.TarInfo),
                    "output": lambda k, v: v}

            for var, tree in self.slab_tower.items():

                data = []
                bag["data"] = data
                self._traverse(tree, bag)

                if len(data) > 0:
                    totalsize = 0
                    maxsize = 0
                    minsize = sys.maxsize
                    nslabs = len(data)

                    for tar in data:
                        totalsize += tar.size
                        if tar.size > maxsize:
                            maxsize = tar.size
                        if tar.size < minsize:
                            minsize = tar.size
                else:
                    totalsize = maxsize = minsize = nslabs = 0

                out[var] = (nslabs, totalsize, maxsize, minsize)

            return out

        elif mode == "":

            out = []

            out.append(("version", self.config["version"]))

            dbuf = []
            for n, d in self.config["dims"].items():
                dbuf.append((n,  d["length"]))

            out.append(("dims", tuple(dbuf)))

            vbuf = []
            for n, v in self.config["vars"].items():
                if "shape" in v:
                    vbuf.append((n, v["shape"]))
                else:
                    vbuf.append((n, None))

            out.append(("vars", tuple(vbuf)))
            out.append(("size", os.path.getsize(self.slab_path)))

            return out


class ParallelPyslabsReaderV1(PyslabsReaderV1):
    pass


class MasterPyslabsReaderV1(PyslabsReaderV1):
    pass


##############################
# PUBLIC FUNCTIONS
##############################


# open slab I/O for master process
def master_open(slab_path, num_procs, mode="w", workdir=None):

    if mode == "w":

        slab_path, begin_path, work_path = _write_paths(slab_path, workdir)

        # create root directory
        os.makedirs(work_path, exist_ok=True)
        clean_folder(work_path)

        begin = copy.deepcopy(INIT_BEGIN)
        begin["work_path"] = work_path
        begin["slab_path"] = slab_path
        begin["mode"] = "w"

        pickle_dump(begin_path, begin)
        
        # create root directory
#        os.makedirs(work_path, exist_ok=True)
#        clean_folder(work_path)

        config = copy.deepcopy(INIT_CONFIG)
        config["_control_"]["num_procs"] = num_procs
        config["_control_"]["begin_path"] = begin_path
        config["_control_"]["slab_path"] = slab_path

        return MasterPyslabsWriterV1(work_path, config)

    elif mode == "r":

        _cache.clear()

        if num_procs is not None and num_procs > 1:
            print("ERROR: parallel-read is not supported, but 'num_procs' "
                  "argument is larger than one: %d" % num_procs)
            sys.exit(-1)

        return MasterPyslabsReaderV1(slab_path)

    else:
        raise PE_Open_Unknownmode(mode)


# open slab I/O for non-master processes
def parallel_open(slab_path, mode="w"):

    if mode == "w":

        _, begin_path, _ = _write_paths(slab_path, None)

        start = time.time()
        begin = None
        work_path = None

        while time.time() - start < INIT_TIMEOUT:
            if os.path.isfile(begin_path):
                with io.open(begin_path, "rb") as fp:
                    begin = pickle.load(fp)
                    work_path = begin["work_path"]
                break
            time.sleep(0.1)

        if begin is None:
            raise PE_Init_Nobeginfile(slab_path)

        while time.time() - start < INIT_TIMEOUT:
            cfg_path = os.path.join(work_path, CONFIG_FILE)

            if not os.path.isfile(cfg_path):
                time.sleep(0.1)
                continue

            with io.open(cfg_path, "rb") as fp:
                config = pickle.load(fp)

            break

        return ParallelPyslabsWriterV1(work_path, config)

    elif mode == "r":
        return ParallelPyslabsReaderV1(slab_path)

    else:
        raise PE_Open_Unknownmode(mode)


# the wrapper of "master_open" for convinience
def open(slab_path, mode="r", num_procs=1, workdir=None):

    return master_open(slab_path, num_procs, mode=mode, workdir=workdir)
