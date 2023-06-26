import xarray as xr
import pandas as pd
import numpy as np
import os
import os.path
import re
from datetime import datetime

engine = "scipy"

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

    valid_files.sort()

    return valid_files
    
    
    

    


def loadWRFDataFromDir(input_dir, prefix="wrfout_d01_", time_rng=None):
    

    fnames = listWRFOutputFiles(input_dir, prefix=prefix, append_dirname=True, time_rng=time_rng)
    ds = xr.open_mfdataset(fnames, decode_times=False, engine=engine, concat_dim=["Time"], combine='nested')

    t = [pd.Timestamp("%s %s" % (t[0:10], t[11:19])) for t in ds.Times.astype(str).to_numpy()]
   
    ts = xr.DataArray(
        data = t,
        dims = ["Time"],
    ).rename('time')
  
    ds = xr.merge([ds, ts]).rename({'Time':'time'})

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






