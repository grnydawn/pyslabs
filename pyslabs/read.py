"""Pyslabs slab read module


"""

from pyslabs import slabif
from pyslabs.error import PE_Read_Exeedlength


class VariableReaderV1():
    def __init__(self, tar_file, slab_tower, var_cfg, dim_cfg):

        self.tar_file = tar_file
        self.slab_tower = slab_tower
        self.dim_cfg = dim_cfg
        self.var_cfg = var_cfg
        self.array_shape = tuple(self.var_cfg["shape"])
        self.start = (0,) * len(self.array_shape)
        shape = []
        for s in self.array_shape:
            if s in self.dim_cfg:
                shape.append(self.dim_cfg[s]["length"])

            else:
                shape.append(s)
        self.shape = tuple(shape)

    def _get_slice(self, dim, st, so, se):

        st = self.start[dim] if st is None else st
        so = self.shape[dim] if so is None else so
        se = 1 if se is None else se
            
        if so in self.dim_cfg:
            so = self.dim_cfg[so]["length"]

        return slice(st, so, se)

    def __getitem__(self, key):

        whole = tuple([self._get_slice(dim, None, None, None)
                      for dim in range(len(self.shape))])

        is_slice = True
        nslices = 0

        if isinstance(key, int):
            key = (key,) + whole[1:]
            is_slice = False
            nslices = len(whole) - 1

        elif isinstance(key, slice):
            key = (self._get_slice(0, key.start, key.stop, key.step),) + whole[1:]
            nslices = len(whole)

        else:
            buf = []

            for i, k in enumerate(key):
                if isinstance(k, int):
                    buf.append(k)
                    if i==0:
                        is_slice = False

                elif isinstance(k, slice):
                    buf.append(self._get_slice(i, k.start, k.stop, k.step))
                    nslices += 1

            if len(self.shape) - len(key) > 0:
                nslices += (len(self.shape) - len(key))
                key = tuple(buf) + whole[len(key):len(self.shape)]

            else:
                key = tuple(buf)
#
#        shape = []
#        for s in self.shape[1:]:
#            if s in self.dim_cfg:
#                shape.append(self.dim_cfg[s]["length"])
#
#            else:
#                shape.append(s)

        is_squeezed, array = slabif.get_array(self.tar_file, self.slab_tower, self.shape[1:],
                                        key[1:], key[0])

        ndim = slabif.ndim(array)
        if ndim > nslices:
            _, array = slabif.squeeze(array)

        elif ndim < nslices:
            array = slabif.expand_dim(array)

        return array
#
#        if self.unstackable:
#            if not is_slice and len(array) == 1 and slabif.ndim(array) == len(self.shape):
#                print("BBBB", self.shape, key, array.shape, is_squeezed)
#                return array[0]
#            else:
#                return array
#
#        elif is_squeezed:
#            if is_slice:
#                print("AAAA", self.shape, key, slabif.shape(array), is_squeezed)
#                return slabif.exapand_dim(array)
#            else:
#                return array
#        else:
#            return array
#        if (self.unstackable and not is_slice and len(array) == 1 and
#            slabif.ndim(array) == len(self.shape)):
#            print("BBBB", self.shape, key, array.shape, is_squeezed)
#            return array[0]
#
#        else:
#            print("AAAA", self.shape, key, slabif.shape(array), is_squeezed)
#            return array
#
