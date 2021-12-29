"""Pyslabs utility module

"""

import os, io, pickle, shutil


supported_array_types = {
    "numpy": (lambda a: (type(a).__name__=="ndarray" and
                        type(a).__module__== "numpy"), "npy")
}


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
