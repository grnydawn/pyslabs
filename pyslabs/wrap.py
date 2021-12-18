import os, pickle

_supported_arrays = {
    "numpy": (lambda a: (type(a).__name__=="ndarray" and
                        type(a).__module__== "numpy"), "npy")
}


def arraytype(slab):
    for atype, (check, ext) in _supported_arrays.items():
        if check(slab):
            return atype, ext
        
    return "pickle", "dat"


def dump(slab, file):

    atype, ext = arraytype(slab)

    if atype == "numpy":
        import numpy as np
        np.save(file, slab)
        return

    try:
        pickle.dump(slab, file)
        file.flush()
        os.fsync(file.fileno())

    except Exception as err:
        with open(file, "wb") as fp:
            pickle.dump(slab, fp)
            fp.flush()
            os.fsync(fp.fileno())


def concat(arrays, atype):

    if not arrays:
        return arrays

    if atype == "numpy":
        import numpy as np
        return np.concatenate(arrays, axis=0)

    try:
        output = arrays[0]
        for item in arrays[1:]:
            output += item

    except TypeError:
        output = arrays[0]
        for item in arrays[1:]:
            if isinstance(output, dict):
                output.update(item)
            else:
                raise Exception("Not supported type for concatenation: %s" %
                                str(type(output)))

    return output


def stack(arrays, atype):

    if not arrays:
        return arrays

    if atype == "numpy":
        import numpy as np
        return np.stack(arrays)

    return type(arrays[0])(arrays)


def load(file, atype):

    if atype == "numpy":
        import numpy as np
        slab = np.load(file, allow_pickle=True)
        return ("numpy", slab)

    try:
        slab = pickle.load(file)

    except Exception as err:
        with open(file, "rb") as fp:
            slab = pickle.load(fp)

    return ("pickle", slab)


def shape(slab):

    atype, ext = arraytype(slab)

    if atype == "numpy":
        import numpy as np
        return slab.shape

    s = []

    while (slab):
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
        import numpy as np
        return slab.ndim

    return(len(shape(slab)))


def length(slab, dim):
    _s = shape(slab)
    return _s[dim]


def squeeze(slab):

    atype, ext = arraytype(slab)

    if atype == "numpy":
        import numpy as np
        return np.squeeze(slab, axis=0)

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
        import numpy as np
        bucket[0] = atype
        bucket[1] = np.concatenate((bucket[1], array[1]), axis=1)
        return

    bucket[0] = atype

    for i, item in enumerate(array[1]):
        bucket[1][i] = bucket[1][i] + item


def _merge(path):

    _d = []
    _f = []
    _atype = None

    for item in sorted(os.listdir(path)):
        _p = os.path.join(path, item)

        if os.path.isdir(_p):
            _d.append(_merge(_p))

        elif os.path.isfile(_p):
            _, atype, _ = item.split(".")

            if _atype is None:
                _atype = atype
                _f.append(load(_p, atype)[1])

            elif _atype != atype:
                raise Exception("Different type exists in a stack: %s != %s" % (_atype, atype))

            else:
                _f.append(load(_p, atype)[1])
            
        else:
            raise Exception("Unknown file type: %s" % _p)

    if _f:
        _m = [_atype, stack(_f, _atype)]

    else:
        _m = [None, None]

    for _i in _d:
        _concat(_m, _i)
        
    return _m


def get_array(var, _squeeze):

    stype, arr = _merge(var.path)

    if _squeeze and length(arr, 0) == 1:
        arr = squeeze(arr)

    return arr
