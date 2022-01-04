"""Pyslabs error module

"""


class Pyslabs_Error(Exception):
    pass


class PE_Open_Unknownmode(Pyslabs_Error):
    pass


class PE_Init_Nobeginfile(Pyslabs_Error):
    pass


class PE_Slab_Shapemismatch(Pyslabs_Error):
    pass


class PE_Begin_Numproc(Pyslabs_Error):
    pass


class PE_Close_Numproc(Pyslabs_Error):
    pass


class PE_Close_Timeout(Pyslabs_Error):
    pass


class PE_Close_Shapemismatch(Pyslabs_Error):
    pass


class PE_Close_Unknowncheck(Pyslabs_Error):
    pass


class PE_Write_Duplicateslabfile(Pyslabs_Error):
    pass


class PE_Read_Exeedlength(Pyslabs_Error):
    pass


class PE_Stabif_Typemismatch(Pyslabs_Error):
    pass


class PE_Util_Typemismatch(Pyslabs_Error):
    pass


class PE_Close_Startindexerror(Pyslabs_Error):
    pass


