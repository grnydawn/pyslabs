"""Pyslabs slab read module


"""

from pyslabs import slabif
from pyslabs.error import PE_Read_Exeedlength


class VariableReaderV1():
    def __init__(self, tar_file, slab_tower, var_cfg, dim_cfg, unstackable):

        self.tar_file = tar_file
        self.slab_tower = slab_tower
        self.dim_cfg = dim_cfg
        self.var_cfg = var_cfg
        self.unstackable = unstackable
        self.array_shape = tuple(self.var_cfg["shape"])
        self.start = (0,) * len(self.array_shape)
        self.shape = tuple(self.array_shape)

    def _get_slice(self, dim, st, so, se):

        st = self.start[dim] if st is None else st
        so = self.shape[dim] if so is None else so
        se = 1 if se is None else se
            
        if so in self.dim_cfg:
            so = self.dim_cfg[so]["length"]

        return slice(st, so, se)

    def __getitem__(self, key):

        # handle unstackable
        if self.unstackable and self.shape[0] == 1:
            key = (0, key)

        whole = tuple([self._get_slice(dim, None, None, None)
                      for dim in range(len(self.shape))])

        is_slice = True

        if isinstance(key, int):
            key = (key,) + whole[1:]
            is_slice = False

        elif isinstance(key, slice):
            key = (self._get_slice(0, key.start, key.stop, key.step),) + whole[1:]

        else:
            buf = []

            for i, k in enumerate(key):
                if isinstance(k, int):
                    buf.append(k)
                    if i==0:
                        is_slice = False

                elif isinstance(k, slice):
                    buf.append(self._get_slice(i, k.start, k.stop, k.step))

            if len(self.shape) - len(key) > 0:
                key = tuple(buf) + whole[len(key):len(self.shape)]

            else:
                key = tuple(buf)

        shape = []
        for s in self.shape[1:]:
            if s in self.dim_cfg:
                shape.append(self.dim_cfg[s]["length"])

            else:
                shape.append(s)

        is_squeezed, array = slabif.get_array(self.tar_file, self.slab_tower, shape,
                                        key[1:], key[0])

        return array
