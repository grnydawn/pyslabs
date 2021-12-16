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
        np.save(file, slab, allow_pickle=False)

    try:
        pickle.dump(slab, file)
        file.flush()
        os.fsync(file.fileno())

    except Exception as err:
        with open(file, "wb") as fp:
            pickle.dump(slab, fp)
            fp.flush()
            os.fsync(fp.fileno())


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
        slab = np.load(file, allow_pickle=False)
        return ("numpy", slab)

    try:
        slab = pickle.load(file)

    except Exception as err:
        with open(file, "rb") as fp:
            slab = pickle.load(fp)

    return ("pickle", slab)


def concat(bucket, array):

    if bucket[1] is None:
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
        bucket[1] = np.concatenate((bucket[1], array[1]))

        return

    bucket[0] = atype

    for i, item in enumerate(array[1]):
        bucket[1][i] = bucket[1][i] + item


def _merge(path):

    _b = []
    _stack = []
    _atype = None

    for item in sorted(os.listdir(path)):
        _p = os.path.join(path, item)

        if os.path.isdir(_p):
            _b.append(_merge(_p))

        elif os.path.isfile(_p):
            _, atype, _ = item.split(".")

            if _atype is None:
                _atype = atype
                _stack.append(load(_p, atype)[1])

            elif _atype != atype:
                raise Exception("Different type exists in a stack: %s != %s" % (_atype, atype))

            else:
                _stack.append(load(_p, atype)[1])
            
        else:
            raise Exception("Unknown file type: %s" % _p)

    if _stack:
        _m = [_atype, stack(_stack, _atype)]

    else:
        _m = [None, None]

    for _i in _b:
        concat(_m, _i)
        
    return _m


def get_array(var):

    stype, arr = _merge(var.path)

    return arr
