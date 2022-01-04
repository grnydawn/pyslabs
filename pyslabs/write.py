"""Pyslabs slab write module


"""

import os
from pyslabs import slabif
from pyslabs.util import arraytype
from pyslabs.error import PE_Slab_Shapemismatch, PE_Write_Duplicateslabfile


class VariableWriterV1():

    def __init__(self, path, config):

        self.path = path
        self.config = config
        self.check_shape = config["check"]["shape"]
        self.auto_stack = config["stack"]["auto"]
        self.level = 0

    def stacking(self, nlevel=1):
        self.level += nlevel

    def write_panel(self, panel, start=None, level=None):

        # TODO: check if panel is multiples of each slab dims
        # TODO: split panel into multiple slabs
        # TODO: generate start and level per each slabs
        # TODO: writes slabs
        pass

    def write(self, slab, start=None, level=None):

        # get slab info
        slab_shape = slabif.shape(slab)

        # normalize start 
        if start is None:
            start = (0,) * len(slab_shape)

        elif isinstance(start, int):
            start = (start,) + (0,) * (len(slab_shape) - 1)

        else:
            start = start + (0,) * (len(slab_shape) - len(start))

        # generate relative path to data file
        rel_path = []

        for st, sh in zip(start, slab_shape):
            rel_path.append(str(st)+"_"+str(sh))

        strlevel = str(self.level) if level is None else str(level)

        if strlevel in self.config["writes"]:
            writes = self.config["writes"][strlevel]

        else:
            writes = {}
            self.config["writes"][strlevel] = writes

        writes["/".join(rel_path)] = (start, slab_shape)

        slab_folder = os.path.join(self.path, *rel_path)

        if not os.path.isdir(slab_folder):
            os.makedirs(slab_folder)

        atype, ext = arraytype(slab)
        slab_path = os.path.join(slab_folder, ".".join([strlevel, atype, ext]))

        if os.path.isfile(slab_path):
            raise PE_Write_Duplicateslabfile(slab_path)

        slabif.dump(slab_path, slab)

        if level is None:
            if self.auto_stack is True:
                self.stacking()

            elif self.auto_stack > 0:
                self.stacking(nlevel=self.auto_stack)
