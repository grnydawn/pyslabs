"""Pyslabs slab read module


"""


class VariableReaderV1():
    def __init__(self, tar_file, slab_tower, var_cfg, dim_cfg, unstackable):

        self.tar_file = tar_file
        self.slab_tower = slab_tower
        self.dim_cfg = dim_cfg
        self.var_cfg = var_cfg
        self.unstackable = unstackable
        self.shape = tuple(self.var_cfg["shape"])
        import pdb; pdb.set_trace()
