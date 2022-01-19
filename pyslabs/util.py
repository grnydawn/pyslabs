"""Pyslabs utility module

"""

import os, io, pickle, shutil

from pyslabs.error import PE_Util_Typemismatch


supported_array_types = {
    "numpy": (lambda a: (type(a).__name__=="ndarray" and
                        type(a).__module__== "numpy"), "npy")
}

DEBUG_NONE, DEBUG_MAJOR, DEBUG_MINOR, DEBUG_INFO, DEBUG_ALL = range(5)
DEBUG_LEVEL = DEBUG_MAJOR
#DEBUG_LEVEL = DEBUG_ALL

class ScalarList():

    def __init__(self, elems=None, type=type(None)):

        self._list = list(elems)
        self._type = type

    def concat(self, elem):

        self._list.append(elem)

        if self._type is type(None):
            self._type = type(elem)

        elif self._type is not type(elem):
            raise PE_Util_Typemismatch("%s != %s" % (self._type, type(elem)))


def arraytype(slab):
    for atype, (check, ext) in supported_array_types.items():
        if check(slab):
            return atype, ext

    return "pickle", "dat"


def pickle_dump(path, obj):
    with io.open(path, "wb") as fp:
        pickle.dump(obj, fp)
        fp.flush()
        os.fsync(fp.fileno())


def clean_folder(folder):
    for files in os.listdir(folder):
        path = os.path.join(folder, files)
        try:
            shutil.rmtree(path)
        except OSError:
            os.remove(path)
