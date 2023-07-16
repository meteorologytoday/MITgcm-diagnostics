import numpy as np
import xarray as xr
import argparse
import pandas as pd
from pathlib import Path
import tool_fig_config

import os.path
import os

from WRFDiag import wrf_load_helper
wrf_load_helper.engine = "netcdf4"

import MITgcmDiff.loadFunctions as lf
import MITgcmDiff.mixed_layer_tools as mlt
import MITgcmDiff.calBudget as cb
import MITgcmDiag.data_loading_helper as dlh

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


        self.msm = MITgcmDiag.dlh.MITgcmSimMetadata(
            start_datetime,
            mitgcm_deltaT,
            mitgcm_dumpfreq,
            mitgcm_data_dir,
            mitgcm_grid_dir,
        )
        self.start_datetime = pd.Timestamp(start_datetime)
        self.deltaT = pd.Timedelta(seconds=deltaT)
        self.dumpfreq = pd.Timedelta(seconds=dumpfreq)
        self.data_dir = data_dir
        self.grid_dir = grid_dir
        
        self.WRF_prefix = WRF_prefix
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
        
    WRF_ds = WRF_ds.mean(dim="time", keep_attrs=True)

    # ========== [ Loading MITgcm data ] ==========
    msm = dlh.MITgcmSimMetadata(args.mitgcm_beg_date, args.mitgcm_deltaT, args.mitgcm_dumpfreq, input_dir, input_dir)
    coo, crop_kwargs = lf.loadCoordinateFromFolderAndWithRange(skrips_meta.msm.grid_dir, nlev=None, lat_rng=args.lat_rng, lon_rng=args.lon_rng)

    MITgcm_data  = dlh.loadAveragedDataByDateRange(beg_dt, end_dt, msm, **crop_kwargs, datasets=["diag_state",], inclusive="right")  # inclusive is right because output at time=t is the average from "before" to t


    return WRF_ds, MITgcm_data, coo 

