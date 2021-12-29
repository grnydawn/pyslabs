"""Pyslabs slab manipulation module


"""

import os, io, pickle

from pyslabs.util import arraytype
import pyslabs.slabif_numpy as numpyif


def shape(slab):

    atype, ext = arraytype(slab)

    if atype == "numpy":
        return numpyif.shape(slab)

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


def dump(path, slab):

    atype, ext = arraytype(slab)

    if atype == "numpy":
        return numpyif.dump(slab, path)

    with io.open(path, "wb") as fp:
        pickle.dump(path, fp)
        fp.flush()
        os.fsync(fp.fileno())
