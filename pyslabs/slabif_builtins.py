"""Pyslabs builtin-data slab manipulation module


"""

import os, io, pickle, itertools

from pyslabs.error import PE_Stabif_Typemismatch
from pyslabs.util import ScalarList, DEBUG_LEVEL, DEBUG_INFO

def length(slab, axis=0):

    return len(slab)


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
        try:
            return type(lower)([lower])

        except TypeError:
            return (lower,)

    if isinstance(stacker, list):
        stacker.append(lower)

    elif isinstance(stacker, tuple):
        stacker += (lower,)

    else:
        import pdb; pdb.set_trace()        

    return stacker


def expand_dim(array):

    return type(array)([array])


def get_blank():
    return tuple()


def concatenate(concater, panel, axis):

    if DEBUG_LEVEL > DEBUG_INFO:
        print("Concatenate IN (concat, panel, axis): ", concater, panel, axis)

    from pyslabs import slabif

    if concater is None:
        return panel

    if type(concater) != type(panel) and not isinstance(concater, ScalarList):
        raise PE_Stabif_Typemismatch("%s != %s" % (type(concater).__name__,
                                    type(panel).__name__))

    if isinstance(concater, ScalarList):
        concater.concat(panel)

    elif isinstance(concater, list):
        if axis > 0:
            if len(concater) != len(panel):
                raise PE_Stabif_Lengthmismatch("%d != %d" % (len(concater),
                                        len(panel)))
            buf = []
            for idx in range(len(concater)):
                buf.append(slabif.concatenate(concater[idx], panel[idx], axis=axis-1))
            concater = buf 

        else:
            concater.extend(panel)

    elif isinstance(concater, tuple):
        if axis > 0:
            if len(concater) != len(panel):
                raise PE_Stabif_Lengthmismatch("%d != %d" % (len(concater),
                                        len(panel)))
            buf = []
            for idx in range(len(concater)):
                buf.append(slabif.concatenate(concater[idx], panel[idx], axis=axis-1))
            concater = tuple(buf) 

        else:
            concater += panel

    else:
        if axis > 0:
            raise PE_Stabif_Wrongaxis()

        concater = ScalarList((concater, panel))


    if DEBUG_LEVEL > DEBUG_INFO:
        print("Concatenate OUT: ", concater)
    return concater



def squeeze(array):
    if DEBUG_LEVEL > DEBUG_INFO:
        print("Squeeze IN: ", array)

    is_squeezed = False

    if isinstance(array, (list, tuple, ScalarList)):
        if len(array) == 1:
            array = array[0]
            is_squeezed = True

    else:
        import pdb; pdb.set_trace()        

    if DEBUG_LEVEL > DEBUG_INFO:
        print("Squeeze OUT(array, is_squeezed): ", array, is_squeezed)

    return is_squeezed, array


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

    if not is_slice:
        return buf[0]

    else:
        return slice2type(buf, type(slab))
