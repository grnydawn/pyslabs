import os, pickle, resource, itertools
import pyslabs.data_numpy as dnp
from collections import OrderedDict
from tarfile import TarInfo

_supported_arrays = {
    "numpy": (lambda a: (type(a).__name__=="ndarray" and
                        type(a).__module__== "numpy"), "npy")
}

_cache = OrderedDict()


def _clear_cache():
    _cache.clear()


def _evict(num_items=1):
    _cache.popitem(last=False)


def arraytype(slab):
    for atype, (check, ext) in _supported_arrays.items():
        if check(slab):
            return atype, ext
        
    return "pickle", "dat"


def dump(slab, file):

    atype, ext = arraytype(slab)

    if atype == "numpy":
        return dnp.dump(slab, file)

    try:
        pickle.dump(slab, file)
        file.flush()
        os.fsync(file.fileno())

    except Exception as err:
        with open(file, "wb") as fp:
            pickle.dump(slab, fp)
            fp.flush()
            os.fsync(fp.fileno())


#def ccconcat(arrays, atype):
#
#    if not arrays:
#        return arrays
#
#    if atype == "numpy":
#        return dnp.concat(arrays, axis=0)
#
#    try:
#        output = arrays[0]
#        for item in arrays[1:]:
#            output += item
#
#    except TypeError:
#        output = arrays[0]
#        for item in arrays[1:]:
#            if isinstance(output, dict):
#                output.update(item)
#            else:
#                raise Exception("Not supported type for concatenation: %s" %
#                                str(type(output)))
#
#    return output


def stack(arrays, atype):

    if not arrays:
        return arrays

    if atype == "numpy":
        return dnp.stack(arrays)

    try:
        return type(arrays[0])(arrays)

    except TypeError:
        return arrays


def _get_slice(slab, key):
    
    if not key:
        return slab

    start, stop, step = key[0].start, key[0].stop, key[0].step

    _stype = type(slab)
    _m = []

    for item in itertools.islice(slab, start, stop, step):
        _m.append(_get_slice(item, key[1:]))

    if _m:
        try:
            _m = _stype(_m)

        except TypeError:
            pass

    return _m


def get_slice(slab, atype, key):

    if slab is None:
        return slab

    if atype == "numpy":
        return dnp.get_slice(slab, key)

    return _get_slice(slab, key)   


def load(tfile, slabobj, atype):

    path = slabobj.path

    if path in _cache:
        return _cache[path]

    # TODO: add caching evict logic
    # usage = resource.getrusage().ru_maxrss

    file = tfile.extractfile(slabobj)

    if atype == "numpy":
        _d = (atype, dnp.load(file))
        _cache[path] = _d
        return _d

    slab = pickle.load(file)

    _d = ("pickle", slab)
    _cache[path] = _d
    return _d


def shape(slab):

    atype, ext = arraytype(slab)

    if atype == "numpy":
        return dnp.shape(slab)

    s = []

    while slab:
        try:
            l = len(slab)

            if l > 0:
                s.append(l)
                slab = slab[0]

            else:
                break
        except TypeError:
            break

    return tuple(s)


def ndim(slab):

    atype, ext = arraytype(slab)

    if atype == "numpy":
        return dnp.ndim(slab)

    return(len(shape(slab)))


def length(slab, dim):
    _s = shape(slab)
    return _s[dim]


def squeeze_dim0(slab, atype):

    if atype == "numpy":
        return dnp.squeeze(slab)

    if length(slab, 0) == 1:
        return slab[0]


def _concat(bucket, array):

    if bucket[1] is None:
        bucket[0] = array[0]
        bucket[1] = array[1]
        return

    if bucket[0] == array[0] or bucket[0] is None:
        atype = array[0]

    elif array[0] is None:
        atype = bucket[0]

    else:
        import pdb; pdb.set_trace()

    if atype == "numpy":
        bucket[0] = atype
        bucket[1] = dnp.concat((bucket[1], array[1]), axis=1)
        return

    bucket[0] = atype

    for i, item in enumerate(array[1]):
        bucket[1][i] = bucket[1][i] + array[1][i]


def _merge(tfile, slabobj):

    _d = []
    _f = []
    _atype = None


    sortedkey = sorted([(int(k.split(".")[0]), k) for k in slabobj], key=lambda x: x[0])

    for (_, key) in sortedkey:
        item = slabobj[key]

        if isinstance(item, dict):
            _d.append(_merge(tfile, item))

        elif isinstance(item, TarInfo):
            _, atype, _ = os.path.basename(item.path).split(".")

            if _atype is None:
                _atype = atype
                # TODO: check size  of loaded array based on key and next key
                _f.append(load(tfile, item, atype)[1])

            elif _atype != atype:
                raise Exception("Different type exists in a stack: %s != %s" % (_atype, atype))

            else:
                _f.append(load(tfile, item, atype)[1])
            
        else:
            raise Exception("Unknown file type: %s" % str(item))

    if _f:
        _m = [_atype, stack(_f, atype)]

    else:
        _m = [None, None]

    for _i in _d:
        _concat(_m, _i)
        
    if _m:
        try:
            _m[1] = type(_m[1][0])(_m[1])

        except TypeError:
            pass

    return _m

def get_array(tfile, slabobj, pack_stack_dim):

    stype, arr = _merge(tfile, slabobj)

    try:
        if pack_stack_dim and arr is not None and length(arr, 0) == 1:
            arr = squeeze_dim0(arr, stype)
    except IndexError as err:
        pass

    return arr
