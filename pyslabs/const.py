"""Pyslabs constant module


"""

name            = "pyslabs"
version         = "0.1.5"
description     = "Pure Python Paralle I/O"
long_description = """Pure Python Parallel I/O that supports large datasets of
 all pickle-able objects and well-known data frameworks"""
author          = "Youngsung Kim"

UNLIMITED           = -1      # unlimited length on stack dimension
INIT_TIMEOUT        = 10      # seconds
FINI_TIMEOUT        = 100     # seconds
SLAB_EXT            = ".slab" # slab file extension
ZLAB_EXT            = ".zlab" # compressed slab file extension

CONFIG_FILE         = "_config_"
FINISH_FILE         = "_finished_"
VARCFG_FILE         = "_varcfg_"

TMP_BEGIN           = "._tmpbegin_" # an extension of a temporary file
TMP_WORK            = "._tmpwork_" # an extension of a temporary directory

INIT_BEGIN = {
    "work_path": None,
    "slab_path": None,
    "mode": None 
}

INIT_CONFIG = {
    "version": 1,
    "dims": {},
    "vars": {},
    "attrs": {},
    "_control_": {
    }
}

INIT_VARCFG = {
    "writes": {},
    "shape": None,
    "check": {},
    "attrs": {},
    "stack": {}
}

INIT_DIMCFG = {
    "attrs": {}
}


