import numpy as np
import xarray as xr
import pandas as pd

import os.path
import os


import MITgcmDiff.loadFunctions

import MITgcmDiag
import MITgcmDiag.data_loading_helper

from WRFDiag import wrf_load_helper
wrf_load_helper.engine = "netcdf4"

class SKRIPSMetadata:

    def __init__(self, 
        start_datetime,
        mitgcm_deltaT,
        mitgcm_dumpfreq,
        mitgcm_data_dir,
        mitgcm_grid_dir,
        WRF_prefix,
        WRF_data_dir,
        WRF_extend_time = pd.Timedelta(days=1),
    ):


        self.msm = MITgcmDiag.data_loading_helper.MITgcmSimMetadata(
            start_datetime,
            mitgcm_deltaT,
            mitgcm_dumpfreq,
            mitgcm_data_dir,
            mitgcm_grid_dir,
        )
        self.start_datetime = pd.Timestamp(start_datetime)
        
        self.WRF_prefix = WRF_prefix
        self.WRF_data_dir = WRF_data_dir
        self.WRF_extend_time = WRF_extend_time


def loadSKRIPS(
    skrips_meta,
    beg_dt,
    end_dt,
):

    # ========== [ Loading WRF data ] ==========
    WRF_ds = wrf_load_helper.loadWRFDataFromDir(
        skrips_meta.WRF_data_dir,
        skrips_meta.WRF_prefix,
        time_rng=[beg_dt, end_dt],
        extend_time=skrips_meta.WRF_extend_time,
    )
        
    #WRF_ds = WRF_ds.mean(dim="time", keep_attrs=True)

    # ========== [ Loading MITgcm data ] ==========
    coo, crop_kwargs = MITgcmDiff.loadFunctions.loadCoordinateFromFolderAndWithRange(skrips_meta.msm.grid_dir, nlev=None)

    MITgcm_data  = MITgcmDiag.data_loading_helper.loadAveragedDataByDateRange(beg_dt, end_dt, skrips_meta.msm, **crop_kwargs, datasets=["diag_state",], inclusive="right", avg=False)  # inclusive is right because output at time=t is the average from "before" to t


    return WRF_ds, MITgcm_data, coo 

