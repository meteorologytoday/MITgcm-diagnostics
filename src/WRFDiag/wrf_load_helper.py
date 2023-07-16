import xarray as xr
import pandas as pd
import numpy as np
import os
import os.path
import re
from datetime import datetime

engine = "scipy"



def findfirst(a):
    return np.argmax(a)

def findlast(a):
    return (len(a) - 1) - np.argmax(a[::-1])


def findArgRange(arr, lb, ub, inclusive="both"):
    if lb > ub:
        raise Exception("Lower bound should be no larger than upper bound")

    if np.any( (arr[1:] - arr[:-1]) <= 0 ):
        raise Exception("input array should be monotonically increasing")

    if inclusive == "both":
        idx = np.logical_and((lb <= arr),  (arr <= ub))
    elif inclusive == "left":
        idx = np.logical_and((lb <= arr),  (arr < ub))
    elif inclusive == "right":
        idx = np.logical_and((lb < arr),  (arr <= ub))
    
    idx_low = findfirst(idx)
    idx_max = findlast(idx)

    return idx_low, idx_max



class WRFSimMetadata:

    def __init__(self, start_datetime, data_freq, frames_per_file):

        self.start_datetime = pd.Timestamp(start_datetime)
        self.data_freq = data_freq
        self.frames_per_file = frames_per_file


def getFilenameAndFrameFromDate(dt, wsm : WRFSimMetadata, prefix="wrfout_d01_", time_fmt="%Y-%m-%d_%H:%M:%S"):
   
    if type(dt) == str:
        dt = pd.Timestamp(dt) 
 
    delta_time = dt - wsm.start_datetime
    frames_diff = delta_time / wsm.data_freq

    file_diff = int( np.floor( frames_diff / wsm.frames_per_file ) )
    frame = int( frames_diff % wsm.frames_per_file )

    _dt = dt + wsm.data_freq * (wsm.frames_per_file * file_diff)
   
    filename = "%s%s.nc" % (prefix, _dt.strftime(time_fmt))
 
    return filename, frame


def listWRFOutputFiles(dirname, prefix="wrfout_d01_", append_dirname=False, time_rng=None):

    valid_files = []
    
    pattern = "^%s(?P<DATETIME>[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2})$" % (prefix,)
    ptn = re.compile(pattern)
    file_times = []

    if time_rng is not None:
        filter_time = True


    for fname in os.listdir(dirname):

        m =  ptn.match(fname)
        if m:
            if append_dirname:
                fname = os.path.join(dirname, fname)


            if filter_time:
                t = pd.Timestamp(datetime.strptime(m.group('DATETIME'), "%Y-%m-%d_%H:%M:%S"))
                if time_rng[0] <= t and t < time_rng[1]:
                    valid_files.append(fname)
            else:
                valid_files.append(fname)

    valid_files.sort()


    return valid_files
    
def _loadWRFTimeOnly(filename):
   
    with xr.open_dataset(filename, engine=engine, decode_times=False,) as ds:
        t = [pd.Timestamp("%s %s" % (t[0:10], t[11:19])) for t in ds.Times.astype(str).to_numpy()]
    
    return t


    


def loadWRFDataFromDir(input_dir, prefix="wrfout_d01_", time_rng=None, extend_time=None):
    

    if time_rng is not None and extend_time is not None:
        load_time_rng = [time_rng[0] - extend_time, time_rng[1] + extend_time]

    else:
        load_time_rng = time_rng

    fnames = listWRFOutputFiles(input_dir, prefix=prefix, append_dirname=True, time_rng=load_time_rng)


    ds = xr.open_mfdataset(fnames, decode_times=False, engine=engine, concat_dim=["Time"], combine='nested')

    t = [pd.Timestamp("%s %s" % (t[0:10], t[11:19])) for t in ds.Times.astype(str).to_numpy()]
   
    ts = xr.DataArray(
        data = t,
        dims = ["Time"],
    ).rename('time')
  
    ds = xr.merge([ds, ts]).rename({'Time':'time'})

    if time_rng is not None:
        
        # Find the range
        t = ds.time.to_numpy()
        flags = (t >= time_rng[0]) & (t < time_rng[1])
        i0 = findfirst(flags)
        i1 = findlast(flags)
        ds = ds.isel(time=slice(i0, i1+1))

    return ds


def loadWRFData(wsm, filename=None):

    
    
    ds = xr.open_dataset(filename, decode_times=False, engine=engine)
    t = [pd.Timestamp("%s %s" % (t[0:10], t[11:19])) for t in ds.Times.astype(str).to_numpy()]
   
    ts = xr.DataArray(
        data = t,
        dims = ["Time"],
    ).rename('time')
  
    ds = xr.merge([ds, ts]).rename({'Time':'time'})

    return ds






