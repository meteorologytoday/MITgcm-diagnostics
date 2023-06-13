import numpy as np
import data_loading_helper as dlh
import MITgcmDiff.loadFunctions as lf
import pandas as pd
import MITgcmDiff.mixed_layer_tools as mlt
import MITgcmDiff.calTurbulentFlux as cal_tbf
from MITgcmDiff.buoyancy_linear import TS2rho
import xarray as xr

import traceback
from multiprocessing import Pool
import multiprocessing
import argparse
from pathlib import Path
import os.path
import os
from MITgcmDiff import buoyancy_linear

parser = argparse.ArgumentParser(
                    prog = 'diag_budgets',
                    description = 'Diagnose daily budget',
)

parser.add_argument('--data-dir', type=str, help='Input data dir.', required=True)
parser.add_argument('--grid-dir', type=str, help='Input grid dir.', required=True)
parser.add_argument('--mitgcm-beg-date', type=str, help='The datetime of iteration zero in mitgcm.', required=True)
parser.add_argument('--mitgcm-deltaT', type=float, help='The timestep (sec) of mitgcm (deltaT).', required=True)
parser.add_argument('--mitgcm-dumpfreq', type=float, help='The timestep (sec) of mitgcm dump frequency.', required=True)
parser.add_argument('--beg-date', type=str, help='The datetime of begin.', required=True)
parser.add_argument('--end-date', type=str, help='The datetime of end.', required=True)
parser.add_argument('--lat-rng', type=float, nargs=2, help='The latitude range.', default=[31.0, 43.0])
parser.add_argument('--lon-rng', type=float, nargs=2, help='The longitude range.', default=[230.0, 244.0])
parser.add_argument('--nlev', type=int, help='The used vertical levels.', default=-1)
parser.add_argument('--nproc', type=int, help='The number of parallel processes.', default=2)
parser.add_argument('--output-dir', type=str, help='Output dir', default="")
args = parser.parse_args()
print(args)

# initialization
msm = dlh.MITgcmSimMetadata(args.mitgcm_beg_date, args.mitgcm_deltaT, args.mitgcm_dumpfreq, args.data_dir, args.grid_dir)
nlev = None if args.nlev == -1 else args.nlev

beg_date = pd.Timestamp(args.beg_date)
end_date = pd.Timestamp(args.end_date)

print("Making output directory: ", args.output_dir)
if not os.path.isdir(args.output_dir):
    print("Create dir: %s" % (args.output_dir,))
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)


print("Load coordinate")
coo, crop_kwargs = lf.loadCoordinateFromFolderAndWithRange(msm.grid_dir, nlev=nlev, lat_rng=args.lat_rng, lon_rng=args.lon_rng)
lat = coo.grid["YC"][:, 0]
lon = coo.grid["XC"][0, :]
lev_W = coo.grid["RF"].flatten()[:-1]

z_T = coo.grid["RC"].flatten()
z_W = coo.grid["RF"].flatten()
mask = coo.grid["maskInC"]

reference_time = pd.Timestamp('1970-01-01')


def work(dt, output_filename):
    
    global msm, args, crop_kwargs
    global coo, lat, lon, z_T, z_W, mask
   
    try: 
        datestr = dt.strftime("%Y-%m-%d")
        print("[%s] Start. " % (datestr,)) 

        data_ave  = dlh.loadAveragedDataByDateRange(dt, dt + pd.Timedelta(24, unit='h'), msm, **crop_kwargs, datasets=["diag_Tbdgt", "diag_Sbdgt", "diag_state",], inclusive="left")

        print("[%s] Data loaded." % (datestr,)) 
        turbflx_T = cal_tbf.computeTurbulentFlux(data_ave["ADVr_TH"] / coo.grid["RAC"], data_ave["WVEL"], data_ave["THETA"], coo)
        turbflx_S = cal_tbf.computeTurbulentFlux(data_ave["ADVr_SLT"] / coo.grid["RAC"], data_ave["WVEL"], data_ave["SALT"], coo)
        turbflx_b = buoyancy_linear.TS2b(turbflx_T, turbflx_S, ref = False)
 
        ds_T = xr.DataArray(
            data=np.expand_dims(turbflx_T, 0),
            dims=["time", "lev_W", "lat", "lon"],
            coords=dict(
                lon=(["lon",], lon),
                lat=(["lat",], lat),
                lev_W=(["lev_W",], lev_W),
                time=[dt,],
                reference_time=reference_time,
            ),
        ).rename("turbflx_T")

        ds_S = xr.DataArray(
            data=np.expand_dims(turbflx_S, 0),
            dims=["time", "lev_W", "lat", "lon"],
            coords=dict(
                lon=(["lon",], lon),
                lat=(["lat",], lat),
                lev_W=(["lev_W",], lev_W),
                time=[dt,],
                reference_time=reference_time,
            ),
        ).rename("turbflx_S")


        ds_b = xr.DataArray(
            data=np.expand_dims(turbflx_b, 0),
            dims=["time", "lev_W", "lat", "lon"],
            coords=dict(
                lon=(["lon",], lon),
                lat=(["lat",], lat),
                lev_W=(["lev_W",], lev_W),
                time=[dt,],
                reference_time=reference_time,
            ),
        ).rename("turbflx_b")


        data = xr.merge([ds_T, ds_S, ds_b])


        print("[%s] Output file: %s" % (datestr, output_filename))
        data.to_netcdf(
            output_filename,
            unlimited_dims=["time",],
            encoding={'time': {'dtype': 'i4'}},
        )

    except Exception as e:

        traceback.print_exc()
        print(e)
        return dt, False

    return dt, True


failed_dates = []
with Pool(processes=args.nproc) as pool:

    dts = pd.date_range(beg_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), freq=pd.Timedelta(3, unit='h'), inclusive="both")

    input_args = []
    for i, dt in enumerate(dts):
        
        dtstr = dt.strftime("%Y-%m-%d")
        output_filename = "%s/turbflx_analysis_%s.nc" % (args.output_dir, dts[i].strftime("%Y-%m-%d_%H"))

        if os.path.exists(output_filename):
            print("[%s] File %s already exists. Do not do this job." % (dtstr, output_filename))

        else:
            input_args.append((dt, output_filename))
        
    
    result = pool.starmap(work, input_args)

print("Tasks finished.")

