import numpy as np
from MITgcmDiff.utils import shift
import MITgcmDiff.Coordinate

def W_interp_T(fi, coo: MITgcmDiff.Coordinate):
    
    boundary = 'fill'

    fo = np.zeros_like(fi)

    upper_data = fi
    lower_data = shift(fi, -1, axis=0, boundary=boundary)

    upper_dz = coo.grid["DRF"]
    lower_dz = shift(coo.grid["DRF"], -1, axis=0, boundary=boundary)

    tmp = (upper_data * lower_dz + lower_data * upper_dz) / (upper_dz + lower_dz)

    fo[0, :, :] = upper_data[0, :, :]
    fo[1:, :, :] = tmp[:-1, :, :] 

    return fo

