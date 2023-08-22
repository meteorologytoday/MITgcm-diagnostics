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
        self.deltaT = pd.Timedelta(seconds=deltaT)
        self.dumpfreq = pd.Timedelta(seconds=dumpfreq)
        self.data_dir = data_dir
        self.grid_dir = grid_dir
        


def genMITgcmFilenames(
    dataset,
    dt,
    msm,
):
    
    iters = getItersFromDate(dt, msm)
    
    filename_first_part = "%s.%010d" % (dataset, iters)

    return "%s.data" % (filename_first_part,) , "%s.meta" % (filename_first_part,)
    


def getItersFromDate(dt, msm : MITgcmSimMetadata):
   
    if type(dt) == str:
        dt = pd.Timestamp(dt) 
 
    delta_seconds = dt - msm.start_datetime
    iters = delta_seconds / msm.deltaT

    if iters % 1 != 0:
        raise Exception("The specified time is not a multiple of deltaT. Please check if deltaT is correct.")
    
    iters = int(iters)
    
    return iters

def getDateFromIters(iters, msm : MITgcmSimMetadata):
    
    return msm.start_datetime + msm.deltaT * iters



def loadAveragedDataByDateRange(beg_dt, end_dt, msm : MITgcmSimMetadata, region=None, lev=(), merge=True, datasets=[], inclusive="right", avg=True):
   
    dts = pd.date_range(beg_dt, end_dt, freq=msm.dumpfreq, inclusive=inclusive)

    data = None

    for i, dt in enumerate(dts):
       
        print("Load date: %s" % (dt.strftime("%Y-%m-%d %H:%M:%S"))) 
        _data = loadDataByDate(dt, msm, region=region, lev=lev, merge=True, datasets=datasets)

        if avg:
            if i == 0:
                data = _data

            else:
                for k in data.keys():
                    data[k] += _data[k]

        else:
            if i == 0:
                data = {
                    k : [_d, ]
                    for k, _d in _data.items()
                }
            else:
                for k in data.keys():
                    data[k].append(_data[k])


    if avg: 
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



def raw_loadAveragedDataByDateRange(beg_dt, end_dt, msm : MITgcmSimMetadata, dataset, inclusive="right"):
   
    dts = pd.date_range(beg_dt, end_dt, freq=msm.dumpfreq, inclusive=inclusive)

    data = None

    bundle = None
    for i, dt in enumerate(dts):
       
        print("Load date: %s" % (dt.strftime("%Y-%m-%d %H:%M:%S"))) 
        _bundle = raw_loadDataByDate(dt, msm, dataset)

        
        if i == 0:
            bundle = _bundle
            data = _bundle['data']

        else:
            data += _bundle['data']
        
        
    data /= len(dts)
    bundle['data'] = data

    return bundle


# A wrapper for rdmds
def raw_loadDataByDate(dt, msm : MITgcmSimMetadata, dataset):
    
    iters = getItersFromDate(dt, msm)
    
    print("Loading file of ", "%s/%s" % (msm.data_dir, dataset,))
    print("Load date: %s" % (dt.strftime("%Y-%m-%d %H:%M:%S"))) 
    bundle = mds.rdmds("%s/%s" % (msm.data_dir, dataset,), iters, returnmeta=True)


    return dict(
        data=bundle[0],
        iters=bundle[1][0],
        metadata=bundle[2],
    )




