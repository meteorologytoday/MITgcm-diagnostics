import MITgcmDiff.loadFunctions
import MITgcmDiff.Operators as op 
import MITgcmDiff.utils as ut
from MITgcmutils import mds
import MITgcmDiff.loadFunctions as lf
import MITgcmDiff.xarrayCoversion as xac
import numpy as np
import pandas as pd


class MITgcmSimMetadata:

    def __init__(self, start_datetime, deltaT, dumpfreq, data_dir, grid_dir):

        self.start_datetime = pd.Timestamp(start_datetime)
        self.deltaT = deltaT
        self.dumpfreq = dumpfreq
        self.data_dir = data_dir
        self.grid_dir = grid_dir
        

def getItersFromDate(dt, msm : MITgcmSimMetadata):
   
    if type(dt) == str:
        dt = pd.Timestamp(dt) 
 
    delta_seconds = (dt - msm.start_datetime).total_seconds()
    iters = delta_seconds / msm.deltaT

    if iters % 1 != 0:
        raise Exception("The specified time is not a multiple of deltaT. Please check if deltaT is correct.")
    
    iters = int(iters)
    
    return iters

def getDateFromIters(iters, msm : MITgcmSimMetadata):
    
    return msm.start_datetime + pd.Timedelta(seconds=msm.deltaT) * iters



def loadAveragedDataByDateRange(beg_dt, end_dt, msm : MITgcmSimMetadata, region=None, lev=(), merge=True, datasets=[], inclusive="both"):
   
    dts = pd.date_range(beg_dt, end_dt, freq=pd.Timedelta(msm.dumpfreq, unit='s'), inclusive=inclusive)

    data = None

    print(dts)
    for i, dt in enumerate(dts):
       
        print("Load date: %s" % (dt.strftime("%Y-%m-%d %H:%M:%S"))) 
        _data = loadDataByDate(dt, msm, region=region, lev=lev, merge=True, datasets=datasets)

        if i == 0:
            data = _data

        else:
            for k in data.keys():
                data[k] += _data[k]


 
    for k in data.keys():
         data[k] /= len(dts)


    return data



def loadDataByDate(dt, msm : MITgcmSimMetadata, region=None, lev=(), merge=True, datasets=[]):
    
    data = dict()

    iters = getItersFromDate(dt, msm)
    
    for k in datasets:
        
        print("Loading file of ", k)
        print("Load date: %s" % (dt.strftime("%Y-%m-%d %H:%M:%S"))) 


        kwargs = dict(
            region=region,
            returnmeta=True,
        )

        if k in ["diag_state", "diag_Tbdgt"]:
            kwargs["lev"] = lev

        bundle = mds.rdmds("%s/%s" % (msm.data_dir, k,), iters, **kwargs)
        _data = lf.postprocessRdmds(bundle)


        if merge:
            for varname, vardata in _data.items():
                data[varname] = vardata

        else:
            data[k] = _data


    return data




