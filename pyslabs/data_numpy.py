import numpy as np
from io import BytesIO

def dump(slab, file):
    return np.save(file, slab)


def concat(arrays, axis=0):
    try:
        return np.concatenate(arrays, axis=axis)
    except Exception as err:
        import pdb; pdb.set_trace()
        print(err)


def stack(arrays):
    return np.stack(arrays)


def load(file):
    bytes = BytesIO()
    bytes.write(file.read())
    bytes.seek(0)
    return np.load(bytes)


def shape(slab):
    return slab.shape


def ndim(slab):
    return slab.ndim


def length(slab, axis):
    _s = shape(slab)
    return _s[axis]


def squeeze(slab):
    return np.squeeze(slab, axis=0)


def get_slice(slab, key):
    return slab.__getitem__(tuple(key))
