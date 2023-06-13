import numpy as np
import data_loading_helper as dlh
import MITgcmDiff.loadFunctions as lf
import pandas as pd
import MITgcmDiff.mixed_layer_tools as mlt
import MITgcmDiff.calBudget as cb
from MITgcmDiff.buoyancy_linear import TS2rho
import xarray as xr

import traceback
from multiprocessing import Pool
import multiprocessing
import argparse
from pathlib import Path
import os.path
import os

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

        data_snp1 = dlh.loadDataByDate(dt - pd.Timedelta(days=1), msm, **crop_kwargs, datasets=["diag_snaps_state",])
        data_snp2 = dlh.loadDataByDate(dt, msm, **crop_kwargs, datasets=["diag_snaps_state",])
        data_ave  = dlh.loadAveragedDataByDateRange(dt, dt + pd.Timedelta(days=1), msm, **crop_kwargs, datasets=["diag_Tbdgt", "diag_2D", "diag_state",], inclusive="left")

        print("[%s] Data loaded." % (datestr,)) 
       
        # Load snapshots
        print("[%s] Compute MLD." % (datestr,)) 
        rho1 = TS2rho(data_snp1["THETA"], data_snp1["SALT"])
        rho2 = TS2rho(data_snp2["THETA"], data_snp2["SALT"])
        MLD1_threshold = mlt.findMLD_rho(rho1, z_T, mask=mask, Nz_bot=coo.grid["Nz_bot"])
        MLD2_threshold = mlt.findMLD_rho(rho2, z_T, mask=mask, Nz_bot=coo.grid["Nz_bot"])

        print("[%s] Compute heat budget..." % (datestr,)) 
        ds = cb.computeHeatTendency(data_ave, coo, return_xarray = True)


        print("[%s] Compute ML heat budget..." % (datestr,))
        data = dict()
        for varnameA, varnameB in [
            ("G_ttl",   "TOTTTEND"),
            ("G_adv",   "TEND_ADV"),
            ("G_diffx", "TEND_DIFFx"),
            ("G_diffy", "TEND_DIFFy"),
            ("G_diffz", "TEND_DIFFz"),
            ("G_sw",    "TEND_SWFLX"),
            ("G_lw",    "TEND_LWFLX"),
            ("G_sen",   "TEND_SENFLX"),
            ("G_lat",   "TEND_LATFLX"),
            ("G_frz",   "TEND_FRZFLX"),
            ("G_dil",   "TEND_TDIFLX"),
            ("G_rlx",   "TEND_RLXFLX"),
            ("G_mas",   "TEND_MASFLX"),
            ("G_nsw",   "TEND_SFCFLX"),
            ("G_mflx",  "TEND_SFC_WTHMASS"),
            ("G_res",   "TEND_RES"),
            ("DIFF_oceQnet",   "DIFF_oceQnet"),
        ]:
            print("Var: ", varnameA)
            data[varnameA] = mlt.computeMLMean(ds[varnameB].to_numpy(), MLD2_threshold, z_W, mask=mask, fill_value=np.nan)

        MLT1 = mlt.computeMLMean(data_snp1["THETA"], MLD1_threshold, z_W, mask=mask, fill_value=np.nan)
        MLT2 = mlt.computeMLMean(data_snp2["THETA"], MLD2_threshold, z_W, mask=mask, fill_value=np.nan)
        data["dMLTdt"] = (MLT2 - MLT1) / 86400.0

        data["G_ent"] = (
              mlt.computeMLMean(data_snp1["THETA"], MLD2_threshold, z_W, mask=mask, fill_value=np.nan)  
            - mlt.computeMLMean(data_snp1["THETA"], MLD1_threshold, z_W, mask=mask, fill_value=np.nan)  
        ) / 86400.0

        data["dMLDdt"] = (MLD2_threshold - MLD1_threshold) / 86400.0
        data["MLD"]    = (MLD1_threshold + MLD2_threshold) / 2.0
        data["MLT"]    = (MLT1 + MLT2) / 2.0
        data["G_adv_mflx"] = data["G_adv"] + data["G_mflx"]
       
        data["G_hdiff"] = data["G_diffx"] + data["G_diffy"]
 
        data["SST"] = data_ave["THETA"][1, :, :]

        xr_data = []
        for _varname, _data in data.items():
            
            xr_data.append(xr.DataArray(
                data=np.expand_dims(_data, 0),
                dims=["time", "lat", "lon"],
                coords=dict(
                    lon=(["lon",], lon),
                    lat=(["lat",], lat),
                    time=[dt,],
                    reference_time=reference_time,
                ),
            ).rename(_varname))

           

        ds = xr.merge(xr_data)

        print("[%s] Output file: %s" % (datestr, output_filename))
        ds.to_netcdf(
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

    dts = pd.date_range(beg_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), inclusive="both")

    input_args = []
    for i, dt in enumerate(dts):
        
        dtstr = dt.strftime("%Y-%m-%d")
        output_filename = "%s/heat_budget_analysis_%s.nc" % (args.output_dir, dts[i].strftime("%Y-%m-%d"))

        if os.path.exists(output_filename):
            print("[%s] File %s already exists. Do not do this job." % (dtstr, output_filename))

        else:
            input_args.append((dt, output_filename))
        
    
    result = pool.starmap(work, input_args)

print("Tasks finished.")

