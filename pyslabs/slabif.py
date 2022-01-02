"""Pyslabs slab manipulation module


"""

import os, io, pickle, itertools, pprint

from pyslabs.util import arraytype
from collections import OrderedDict
import pyslabs.slabif_numpy as npif
import pyslabs.slabif_builtins as bif

_cache = OrderedDict()


def length(slab, axis=0):

    atype, ext = arraytype(slab)

    if atype == "numpy":
        l = npif.length(slab, axis=dim)

    else:
        l = bif.shape(slab, axis=dim)

    return l


def shape(slab):

    atype, ext = arraytype(slab)

    if atype == "numpy":
        sp = npif.shape(slab)

    else:
        sp = bif.shape(slab)

    return sp


def dump(path, slab):
    print("Slabif dump IN (path, slab): ", path, slab)

    atype, ext = arraytype(slab)

    if atype == "numpy":
        out = npif.dump(path, slab)

    else:
        out = bif.dump(path, slab)

    return out


def load(tar_file, slab_info, atype):

    path = slab_info.path

    if path in _cache:
        return _cache[path]

    tar_file = tar_file.extractfile(slab_info)

    if atype == "numpy":
        slab = npif.load(tar_file)

    else:
        slab = bif.load(tar_file)

    _cache[path] = slab

    return _cache[path]


def stack(upper, lower):

    atype, ext = arraytype(lower)

    try:
        if atype == "numpy":
            array = npif.stack(upper, lower)

        else:
            array = bif.stack(upper, lower)
    except Exception as err:
        import pdb; pdb.set_trace()
        print(err)

    return array


def concatenate(concater, panel, axis):

    atype, ext = arraytype(concater)

    try:
        if atype == "numpy":
            array = npif.concatenate(concater, panel, axis)

        else:
            array = bif.concatenate(concater, panel, axis)
    except Exception as err:
        import pdb; pdb.set_trace()
        print(err)

    return array

def squeeze(array):

    atype, ext = arraytype(array)

    if atype == "numpy":
        is_sq, sq= npif.squeeze(array)

    else:
        is_sq, sq = bif.squeeze(array)

    return is_sq, sq


# slice of array
def get_slice(array, key):

    if array is None:
        return slab

    atype, ext = arraytype(array)

    if atype == "numpy":
        sl = npif.get_slice(array, key)

    else:
        sl = bif.get_slice(array, key)

    return sl


def get_blank(atype):

    if atype == "numpy":
        bl = npif.get_blank()

    else:
        bl = bif.get_blank()

    return bl


def get_column(tar_file, slab_tower, stack_key, slab_key):


    print("Get_Column IN (slat_tower, stack_key, slab_key): ", slab_tower.keys(), stack_key, slab_key)

    #For a positive step, r[i] = start + step*i where i >= 0 and r[i] < stop.
    #For a negative step, r[i] = start + step*i, but the constraints are i >= 0 and r[i] > stop.

    slab_type = None
    keys = sorted(slab_tower.keys(), key=lambda x:int(x.split(".")[0]))

    if isinstance(stack_key, int):
        is_slice = False
        stack_slice = slice(stack_key, stack_key+1, 1)

    else:
        is_slice = True
        stack_slice = stack_key

    if isinstance(stack_key, int):
        is_slice = False
        k0 = stack_key
        if k0 < 0:
            k0st = len(keys) + k0
            stack_slice = slice(k0st, k0st+1, 1)

        else:
            stack_slice = slice(k0, k0+1, 1)

    else:
        is_slice = True
        st = stack_key.start
        if st < 0: st = st + len(keys)
        so = stack_key.stop
        if so < 0: so = so + len(keys)
        se = stack_key.step
        if se < 0:
            raise PE_Slabif_Negativestep()

        stack_slice = slice(st, so, se)

    stacker = None

    print("Gen_Column itertool islice: ", stack_slice)

    for key in itertools.islice(keys, stack_slice.start, stack_slice.stop,
                                stack_slice.step):
        tinfo = slab_tower[key]
        _, _stype, _ = key.split(".")

        if slab_type is None:
            slab_type = _stype

        elif slab_type != _stype:
            raise PE_Read_Slabtypemismatch("%s != %s" % (slab_type, _stype))

        slab_slice = get_slice(load(tar_file, tinfo, slab_type), slab_key)
        stacker = stack(stacker, slab_slice)

    #if not is_slice:
    #    stacker = squeeze(stacker)

    print("Get_Column Out (squeezed, stacker): ", False, stacker)
    return False, stacker

def get_array(tar_file, slab_tower, shape, slab_key, stack_key, new_key=None):
    print("\nGet_array IN(tower, shape, slab_key, stack_key, new_key): ", slab_tower.keys(), shape, slab_key, stack_key, new_key)

    if len(slab_key) == 0:
        is_squeezed, column =  get_column(tar_file, slab_tower, stack_key, new_key)
        print("Get_array Column: \n")
        pprint.pprint(column)

        return is_squeezed, column

    cidxes = sorted([int(k) for k in slab_tower.keys()])
    nidxes = list(cidxes)[1:] + [shape[0]]

    if isinstance(slab_key[0], int):
        is_slice = False
        k0 = slab_key[0]
        if k0 < 0:
            k0st = shape[0] + k0
            ckey = slice(k0st, k0st+1, 1)

        else:
            ckey = slice(slab_key[0], slab_key[0]+1, 1)

    else:
        is_slice = True
        st = slab_key[0].start
        if st < 0: st = st + shape[0]
        so = slab_key[0].stop
        if so < 0: so = so + shape[0]
        se = slab_key[0].step
        if se < 0:
            raise PE_Slabif_Negativestep()

        ckey = slice(st, so, se)

    concater = None

    atype = None
    offset = 0

    for cidx, nidx in zip(cidxes, nidxes):

        sub_tower = slab_tower[str(cidx)]

        if nidx <= ckey.start:
            continue

        if cidx >= ckey.stop:
            break

        if cidx <= ckey.start and ckey.start < nidx:
            a = ckey.start - cidx

            if ckey.stop >= nidx:
                b = nidx - cidx

            else:
                b = ckey.stop - cidx

        elif cidx <= ckey.stop and ckey.stop <= nidx:
            a = offset
            b = ckey.stop - cidx

        else:
            a = offset
            b = nidx - cidx

        offset = (ckey.step - (b - a) % ckey.step) % ckey.step

        next_key = list() if new_key is None else list(new_key)

        if is_slice:
            last_key = slice(a, b, ckey.step)

        else:
            lslice = [s for s in range(a, b, ckey.step)]
            if len(lslice) == 1:
                last_key = lslice[0]

            else:
                raise PE_Slabif_Wrongslicing()

        next_key.append(last_key)

        is_squeezed, panel = get_array(tar_file, sub_tower, shape[1:],
                                slab_key[1:], stack_key, next_key)
        if concater is None:
            concater = panel

        else:
            axis = (len(next_key)-1) if is_squeezed else len(next_key)
            print("Slab Key, next_key, axis: ", slab_key[1:], next_key, axis)
            concater = concatenate(concater, panel, axis)

    is_squeezed = False

    if not is_slice:
        print("TYPE: ", type(concater).__name__)
        if concater is None: import pdb; pdb.set_trace()
        is_squeezed, concater = squeeze(concater)

    if concater is None:
        concater = get_blank(atype)

    print("Get_array OUT: ", concater)
    return is_squeezed, concater
