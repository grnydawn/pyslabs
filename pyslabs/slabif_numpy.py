"""Pyslabs numpy slab manipulation module


"""

import numpy as np
from io import BytesIO
from pyslabs.util import DEBUG_LEVEL, DEBUG_INFO


def length(slab, axis=0):
    return np.size(slab, axis)


def shape(ndarr):
    return ndarr.shape


def dump(path, ndarr):
    return np.save(path, ndarr)


def load(file):
    bio = BytesIO()
    bio.write(file.read())
    bio.seek(0) 
    return np.load(bio)


def get_slice(slab, key):
    return slab.__getitem__(tuple(key))


def stack(stacker, lower):

    if stacker is None:
        stacker = np.expand_dims(lower, axis=0)

    else:
        stacker = np.append(stacker, np.expand_dims(lower,0), 0)

    return stacker


def expand_dim(slab):
    return np.expand_dims(slab, axis=0)


def concatenate(concater, panel,  axis=0):

    if concater is None:
        return panel

    if type(concater) != type(panel):
        raise PE_Stabif_Typemismatch("%s != %s" % (type(concater).__name__,
                                    type(panel).__name__))

    return np.concatenate((concater, panel), axis=axis)


def squeeze(array):
    if DEBUG_LEVEL > DEBUG_INFO:
        print("Squeeze IN: ", array)

    is_squeezed = False

    if array.shape[0] == 1:
        array = array[0]
        is_squeezed = True

    if DEBUG_LEVEL > DEBUG_INFO:
        print("Squeeze OUT(array, is_squeezed): ", array, is_squeezed)

    return is_squeezed, array
