"""Pyslabs numpy slab manipulation module


"""

import numpy as np
from io import BytesIO


def shape(ndarr):
    return ndarr.shape


def dump(path, ndarr):
    return np.save(path, ndarr)

