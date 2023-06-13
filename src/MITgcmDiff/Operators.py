import numpy as np
import MITgcmDiff.Coordinate
from MITgcmDiff.utils import shift

# Convention follows
#https://mitgcm.readthedocs.io/en/latest/outp_pkgs/outp_pkgs.html#mitgcm-kernel-available-diagnostics-list

def T_DIVx_U(fi, coo: MITgcmDiff.Coordinate, weighted = True, boundary='fill'):

    if weighted:    
        fo = ( shift(fi, -1, axis=2, boundary=boundary) - fi ) / coo.grid["DVOLT"]
    else:
        _fi = fi * coo.grid["DYG"]
        fo = ( shift(_fi, -1, axis=2, boundary=boundary) - _fi ) / coo.grid["RAC_slab"]


    return fo


def T_DIVy_V(fi, coo: MITgcmDiff.Coordinate, weighted = True, boundary='fill'):

    if weighted:    
        fo = ( shift(fi, -1, axis=1, boundary=boundary) - fi ) / coo.grid["DVOLT"]
    else:
        _fi = fi * coo.grid["DXG"]
        fo = ( shift(_fi, -1, axis=1, boundary=boundary) - _fi ) / coo.grid["RAC_slab"]


    return fo


def T_DIVz_W(fi, coo: MITgcmDiff.Coordinate, weighted = True, boundary='fill'):

    if weighted:
        fo = ( fi - shift(fi, -1, axis=0, boundary=boundary) ) / coo.grid["DVOLT"]
    else:
        fo = ( fi - shift(fi, -1, axis=0, boundary=boundary) ) / coo.grid["DRF"]


    return fo


    
 






