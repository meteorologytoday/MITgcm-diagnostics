import traceback
import numpy as np
import xarray as xr
import argparse
import pandas as pd
from pathlib import Path
import tool_fig_config

from multiprocessing import Pool
import multiprocessing
import os.path
import os
import traceback

from WRFDiag import wrf_load_helper
wrf_load_helper.engine = "netcdf4"


import MITgcmDiff.loadFunctions as lf
import MITgcmDiff.mixed_layer_tools as mlt
import MITgcmDiff.calBudget as cb
import MITgcmDiag.data_loading_helper as dlh

parser = argparse.ArgumentParser(
                    prog = 'plot_skill',
                    description = 'Plot prediction skill of GFS on AR.',
)

parser.add_argument('--date-rng', type=str, nargs=2, help='Date range.', required=True)
parser.add_argument('--skip-hrs', type=int, help='The skip in hours to do the next diag.', required=True)
parser.add_argument('--avg-hrs', type=int, help='The length of time to do the average in hours.', default=np.nan)
#parser.add_argument('--data-freq-hrs', type=int, help='The data frequency in hours.', required=True)
parser.add_argument('--sim-names', type=str, nargs='*', help='Simulation names', default=[])

parser.add_argument('--input-dirs', type=str, nargs='+', help='Input dirs.', required=True)
parser.add_argument('--output', type=str, help='Output dir', default="")
parser.add_argument('--output-nc', type=str, help='Output dir', default="")
parser.add_argument('--nproc', type=int, help='Number of processors.', default=1)
parser.add_argument('--lat-rng', type=float, nargs=2, help='Latitudes in degree', default=[20, 52])
parser.add_argument('--lon-rng', type=float, nargs=2, help='Longitudes in degree', default=[360-180, 360-144])
parser.add_argument('--overwrite', action="store_true")
parser.add_argument('--no-display', action="store_true")
args = parser.parse_args()
print(args)

if np.isnan(args.avg_hrs):
    print("--avg-hrs is not set. Set it to --skip-hrs = %d" % (args.skip_hrs,))
    args.avg_hrs = args.skip_hrs

skip_hrs = pd.Timedelta(hours=args.skip_hrs)
avg_hrs  = pd.Timedelta(hours=args.avg_hrs)
dts = pd.date_range(args.date_rng[0], args.date_rng[1], freq=skip_hrs, inclusive="left")

args.lon = np.array(args.lon_rng) % 360.0

lat_n, lat_s = np.amax(args.lat_rng), np.amin(args.lat_rng)
lon_w, lon_e = np.amin(args.lon_rng), np.amax(args.lon_rng)

measured_variables_lnd = ['PREC_ACC_C', 'PREC_ACC_NC', 'SNOW_ACC_NC',]
measured_variables_ocn = ['SST', 'HFX', 'LH']

acc_variables = ['PREC_ACC_C', 'PREC_ACC_NC', 'SNOW_ACC_NC',]


if len(args.sim_names) == 0:
    args.sim_names = args.input_dirs
elif len(args.sim_names) != len(args.input_dirs):
    raise Exception("--sim-names is provided but the number of input does not match the --input-dirs")

print("==================================")
print("Output file: %s" % (args.output,))
print("Date range: ", dts[0], " to ", dts[-1])
print("Skip : ", skip_hrs)
print("Avg  : ", avg_hrs)
print("Latitude  box: %.2f %.2f" % (lat_s, lat_n))
print("Longitude box: %.2f %.2f" % (lon_w, lon_e))

for i, input_dir in enumerate(args.input_dirs):
    print("The %d-th input folder: %s" % (i, input_dir,))

print("==================================")


#print("Create dir: %s" % (args.output_dir,))
prec_acc_dt = None

def doWork(t_idx, beg_dt, end_dt):
    
    beg_dtstr = beg_dt.strftime("%Y-%m-%d_%H")
    end_dtstr = end_dt.strftime("%Y-%m-%d_%H")
   
    print("Doing date range: [%s, %s]" % (beg_dtstr, end_dtstr))

    try:

        ref_time = None
        subdata = []
        
        for i, input_dir in enumerate(args.input_dirs):

            print("Load the %d-th folder: %s" % (i, input_dir,))
            _ds = wrf_load_helper.loadWRFDataFromDir(input_dir, prefix="wrfout_d01_", time_rng=[beg_dt, end_dt], extend_time=pd.Timedelta(hours=3))
        
            if i == 0:
                ref_time = _ds.time.to_numpy()
                lon = _ds.coords["XLONG"].isel(time=0) % 360
                lat = _ds.coords["XLAT"].isel(time=0)

                print("Loaded time: ")
                print(ref_time)
                
                time_vec.append(ref_time[0])


            if i > 0:
                if any(ref_time != _ds.time.to_numpy()):
                    raise Exception("Time is not consistent between %s and %s" % (args.input_dirs[0], input_dir,))


            prec_acc_dt = _ds.attrs['PREC_ACC_DT'] * 60
            _ds = xr.merge([ _ds[v] for v in measured_variables_lnd + measured_variables_ocn + ['LANDMASK']])
            _ds = _ds.mean(dim="time", keep_attrs=True)
            
            _ds = _ds.where(
                ( lat > lat_s ) 
                & ( lat < lat_n )
                & ( lon > lon_w )
                & ( lon < lon_e )
            )

            d = {}

            #print("Sum of landmask : ", np.sum(_ds.LANDMASK == 1).to_numpy())
            #print("Sum of ocnmask : ",  np.sum(_ds.LANDMASK == 0).to_numpy())

            for varname in measured_variables_lnd:
            
                d[varname] = _ds[varname].where(_ds.LANDMASK == 1).weighted(np.cos(lat * np.pi / 180)).mean(dim=['south_north', 'west_east'], skipna=True).to_numpy()
            
            
            for varname in measured_variables_ocn:
                d[varname] = _ds[varname].where(_ds.LANDMASK == 0).weighted(np.cos(lat * np.pi / 180)).mean(dim=['south_north', 'west_east'], skipna=True).to_numpy()


            for varname in acc_variables:
                d[varname] *= 1e-3 / prec_acc_dt

            subdata.append(d)

        result = dict(t_idx=t_idx, t=beg_dt, subdata=subdata, prec_acc_dt=prec_acc_dt, status="OK")
    
    except Exception as e:
        
        traceback.print_exc()

        # Add nan
        subdata = []

        for i, input_dir in enumerate(args.input_dirs):
            d = {}
            for varname in measured_variables_lnd + measured_variables_ocn + acc_variables:
                d[varname] = np.nan
            
            subdata.append(d)
        
        result = dict(t_idx=t_idx, t=beg_dt, subdata=subdata, status="ERROR")
        

    return result



#Path(args.output_dir).mkdir(parents=True, exist_ok=True)

if args.output_nc != "" and Path(args.output_nc).is_file():

    print("File ", args.output_nc, " already exists. Load it!")
    ds = xr.open_dataset(args.output_nc)

else: 
    data = [None for _ in dts]
    time_vec = []

    with Pool(processes=args.nproc) as pool:

        
        input_args = []

        for i, beg_dt in enumerate(dts):
            
            end_dt = beg_dt + avg_hrs

            beg_dtstr = beg_dt.strftime("%Y-%m-%d_%H")
            end_dtstr = end_dt.strftime("%Y-%m-%d_%H")
     
            input_args.append((i, beg_dt, end_dt,))

        print("Distributing Jobs...")
        results = pool.starmap(doWork, input_args)

        for i, result in enumerate(results):

            print("Extracting result %d, that contains t_idx=%d" % (i, result['t_idx'],))
                
            data[result['t_idx']] = result['subdata']

            if result['status'] == "OK":
                if prec_acc_dt is None:
                    prec_acc_dt = result['prec_acc_dt']
                else:
                    if prec_acc_dt != result['prec_acc_dt']:
                        raise Exception('Some file does not have the same prec_acc_dt. %d and %d' % (prec_acc_dt, result['prec_acc_dt']))
            else:
                
                print("Something wrong with time: %s (%s)" % (result['t'].strftime("%Y-%m-%d_%H"), result['status'], )) 
                    
    
    newdata = {
        k : np.zeros((len(args.input_dirs), len(dts),))
        for k in measured_variables_ocn + measured_variables_lnd
    }

    for i, input_dir in enumerate(args.input_dirs):
        
        print("Doing reorganizing of the %d-th folder: %s" % (i, input_dir,))
        
        for k, _ in newdata.items():
            
            for j, subdata in enumerate(data): # for each time
                newdata[k][i, j] = data[j][i][k]
            
    ds = xr.Dataset(
        data_vars={
            k : (["run", "time", ], v) for k, v in newdata.items()
        },
        coords=dict(
            time=dts,
            reference_time=pd.Timestamp('2001-01-01'),
        ),
        attrs=dict(
            PREC_ACC_DT = prec_acc_dt,
        )
    )

    if args.output_nc != "":
        print("Output to file: ", args.output_nc)
        ds.to_netcdf(args.output_nc)

print("Load matplotlib...")

import matplotlib as mpl
if args.no_display is False:
    print("Load TkAgg")
    mpl.use('TkAgg')
else:
    print("Load Agg")
    mpl.use('Agg')
    mpl.rc('font', size=15)
 
    
mpl.use('Agg')
# This program plots the AR event
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.patches import Rectangle
import matplotlib.transforms as transforms
from matplotlib.dates import DateFormatter
import matplotlib.ticker as mticker


fig, ax = plt.subplots(4, 1, figsize=(6, 12))

for i, input_dir in enumerate(args.input_dirs):
    _ds = ds.isel(run=i)

    TTL_PREC = (_ds['PREC_ACC_C'] + _ds['PREC_ACC_NC'])
    ACC_PREC = np.cumsum(TTL_PREC) * _ds.attrs["PREC_ACC_DT"] * 1e3
    TTL_PREC *= 1e3 * 86400
   
    LHFLX = _ds['LH']
    
    SST = _ds['SST'] - 273.15
    
    SST[0] = np.nan
    LHFLX[0] = np.nan
    
    ax[0].plot(ds.time, TTL_PREC, label=args.sim_names[i], linewidth=1)
    ax[1].plot(ds.time, ACC_PREC, label=args.sim_names[i], linewidth=1)
    ax[2].plot(ds.time, LHFLX,    label=args.sim_names[i], linewidth=1)
    ax[3].plot(ds.time, SST, label=args.sim_names[i], linewidth=1)


ax[0].set_ylabel('Rain rate [mm/day]')
ax[1].set_ylabel('Accumulated rainfall [mm]')
ax[2].set_ylabel('Latent heat flux [$\\mathrm{W}/\\mathrm{m}^2$]')
ax[3].set_ylabel("Mean SST [${}^{\\circ}\\mathrm{C}$]")


    
ax[0].legend()
for _ax in ax.flatten():
    _ax.grid()
    _ax.xaxis.set_major_formatter(DateFormatter("%Y\n%m/%d"))

if args.output != "":
    print("Writing to file: ", args.output)
    fig.savefig(args.output, dpi=600)

if args.no_display is False:
    plt.show()

print("done")

