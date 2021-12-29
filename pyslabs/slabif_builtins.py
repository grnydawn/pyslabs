"""Pyslabs builtin-data slab manipulation module


"""

import os, io, pickle, itertools

def length(slab, axis=0):
    import pdb; pdb.set_trace()


def slice2type(_slice, _type):

    return _type(_slice)


def shape(slab):

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

    with io.open(path, "wb") as fp:
        pickle.dump(slab, fp)
        fp.flush()
        os.fsync(fp.fileno())

def load(tar_file):

    return pickle.load(tar_file)


def stack(stacker, lower):

    if stacker is None:
        return type(lower)([lower])

    if type(stacker) != type(lower):
        raise PE_Stabif_Typemismatch("%s != %s" % (type(stacker).__name__,
                                    type(lower).__name__))

    if isinstance(stacker, list):
        stacker.append(lower)

    elif isinstance(stacker, tuple):
        stacker += (lower,)

    else:
        import pdb; pdb.set_trace()        

    return stacker


def squeeze(array):

    if isinstance(array, (list, tuple)):
        if len(array) == 1:
            array = array[0]

    else:
        import pdb; pdb.set_trace()        

    return array


def get_slice(slab, key):

    from pyslabs import slabif

    if not key:
        return slab

    if isinstance(key, (int, slice)):
        key = (key,)

    if isinstance(key[0], int):
        is_slice = False
        start, stop, step = key[0], key[0]+1, 1

    elif isinstance(key[0], slice):
        is_slice = True
        start, stop, step = key[0].start, key[0].stop, key[0].step

    buf = []

    for item in itertools.islice(slab, start, stop, step):
        if len(key) > 1:
            item_slice = slabif.get_slice(item, key[1:])
            buf.append(item_slice)
        else:
            buf.append(item)

    if not is_slice and len(buf) == 1:
        return buf[0]

    else:
        return slice2type(buf, type(slab))
