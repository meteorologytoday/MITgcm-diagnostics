from MITgcmutils import mds
import MITgcmDiff.Operators as Operators
from MITgcmDiff.Coordinate import Coordinate
import MITgcmDiff.utils as ut
def genDataDictionary(data, metadata):

    
    #    data      :: AbstractArray,
    #    metadata  :: Dict,
    #    copy_data :: Bool = false,
    #)

    dimlist = metadata["dimlist"]
    
    data_dict = dict()
    for (i, fldname) in enumerate(metadata["fldlist"]):

        trimmed_fldname = fldname.lstrip().rstrip()
        data_dict[trimmed_fldname] = data[i]

    return data_dict

 
def postprocessRdmds(bundle):

    data = bundle[0]
    itrs = bundle[1]
    metadata = bundle[2]
    
   
    return genDataDictionary(data, metadata) 

def loadCoordinateFromFolderAndWithRange(dirname, nlev=None, lat_rng=None, lon_rng=None):


    _coo = loadCoordinateFromFolder(dirname, nlev=nlev)

    lat = _coo.grid["YC"][:, 0]
    lon = _coo.grid["XC"][0, :]
    
    if lat_rng is not None and lon_rng is not None:
 
        region = ut.findRegion_latlon(
            _coo.grid["YC"][:, 0], lat_rng,
            _coo.grid["XC"][0, :], lon_rng,
        )

    else:
        region = (0, len(lon), 0, len(lat))

    print("Region: ", region)

    coo = loadCoordinateFromFolder(dirname, nlev=nlev, region=region)
  
    if nlev is None:
        nlev = len(coo.grid["RC"])
 
    return coo, dict(lev=list(range(nlev)), region=region)

def loadCoordinateFromFolder(dirname, nlev=None, region=None):
    
    data = dict()

    for varname in [
        "DXG",
        "DYG",
        "DXC",
        "DYC",
        "DRC",
        "DRF",
        "RAC",
        "RAZ",
        "XG",
        "YG",
        "XC",
        "YC",
        "RF",
        "RC",
        "maskInC",
        "maskInS",
        "maskInW",
        "hFacC",
        "hFacW",
        "Depth",
    ]:

        data[varname] = mds.rdmds("%s/%s" % (dirname, varname,))

    Ny, Nx = data["DXC"].shape
    Nz, _, _ = data["RC"].shape
    print("(Nz, Ny, Nx) = (%d, %d, %d)" % (Nz, Ny, Nx,))

    if region is not None:
        for k, v in data.items():
            vs = v.shape
            if len(vs) == 3 and vs[1] == Ny and vs[2] == Nx:
                data[k] = v[:, region[2]:region[3], region[0]:region[1]]
            elif len(vs) == 2:
                data[k] = v[region[2]:region[3], region[0]:region[1]]

        print("Update shape.")
        Ny, Nx = data["DXC"].shape
        Nz, _, _ = data["RC"].shape
        print("(Nz, Ny, Nx) = (%d, %d, %d)" % (Nz, Ny, Nx,))



    if nlev is not None:

        for varname in [
            "hFacC",
            "hFacW",
            "RC",
            "DRF",
        ]:

            data[varname] = data[varname][0:nlev, :, :]

        for varname in [
            "RF",
        ]:

            data[varname] = data[varname][0:nlev+1, :, :]

        for varname in [
            "DRC",
        ]:

            data[varname] = data[varname][0:nlev-1, :, :]



    #for varname in ["DRF", "DRC", "RF", "RC"]:
    #    data[varname] = data[varname][:, 0, 0][:, None, None]


    return Coordinate(data)

"""
def loadSkeletonFromFolder(dirname, nlev=None):
    
    data = dict()
    for varname in [
        "XG",
        "YG",
        "XC",
        "YC",
        "RF",
        "RC",
        "DRF",
        "DRC",
        "maskInC",
        "maskInS",
        "maskInW",
    ]:

        data[varname] = mds.rdmds("%s/%s" % (dirname, varname,))


    if nlev is not None:

        for varname in [
            "RC",
        ]:

            data[varname] = data[varname][0:nlev, :, :]

        for varname in [
            "RF",
        ]:

            data[varname] = data[varname][0:nlev+1, :, :]


    for varname in ["DRF", "DRC", "RF", "RC"]:
        data[varname] = data[varname][:, 0, 0][:, None, None]

    return data

"""


if __name__ == "__main__":


    input_dirname = "/data/SO2/SWOT/GRID/BIN"


    coo = loadCoordinateFromFolder(input_dirname, nlev=10)

    for k, v in coo.grid.items():    
        print("%s => " % (k,), v.shape) 
    
    print("Testing shift...")
    for boundary in ["periodic", "fill"]:
        for shift in [0, 1, 5, -1, -5]:
            print("Shift on z axes with shift = %d and boundary = %s" % (shift, boundary, ))
            print(Operators.shift(coo.grid["RF"], shift=shift, axis=0, boundary=boundary))

    #print(coo.grid["XC"][0, :])
    #print(coo.grid["YC"][:, 0])


    
