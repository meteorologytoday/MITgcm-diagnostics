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

import MITgcmDiff.loadFunctions
import data_loading_helper as dlh

parser = argparse.ArgumentParser(
                    prog = 'plot_skill',
                    description = 'Plot prediction skill of GFS on AR.',
)

parser.add_argument('--date-rng', type=str, nargs=2, help='Date range.', required=True)
parser.add_argument('--avg-days', type=int, help='How many days before and after to average.', required=True)
parser.add_argument('--lat', type=float, help='Center of the lat.', required=True)
parser.add_argument('--lon', type=float, help='Center of the lon.', required=True)
parser.add_argument('--rad', type=float, help='Radius of the lat lon.', required=True)
parser.add_argument('--input-dir', type=str, help='Input dir', default="output_diag_budgets")
parser.add_argument('--output-dir', type=str, help='Output dir', default="output_figure_vertical_profile")
parser.add_argument('--nproc', type=int, help='Number of processors.', default=1)
parser.add_argument('--overwrite', action="store_true")
args = parser.parse_args()
print(args)


data_dir = "/data/SO2/SWOT/MARA/RUN4_LY/DIAGS_DLY"
grid_dir = "/data/SO2/SWOT/GRID/BIN"
msm = dlh.MITgcmSimMetadata('2015-01-01', 150.0, data_dir, grid_dir)
nlev = 50
lat_rng = [31.0, 43.0]
lon_rng = [230.0, 244.0] #360 .- [130.0, 116.0
#lon_rng = [235.0, 236.0] #360 .- [130.0, 116.0

print("nlev : %d" % (nlev,))
print("Lat range: ", lat_rng)
print("Lon range: ", lon_rng)

print("Loading data")

coo, crop_kwargs = MITgcmDiff.loadFunctions.loadCoordinateFromFolderAndWithRange(grid_dir, nlev=nlev, lat_rng=lat_rng, lon_rng=lon_rng)




dts = pd.date_range(args.date_rng[0], args.date_rng[1], freq="D", inclusive="both")

args.lon = args.lon % 360.0

lat, lon = args.lat, args.lon

print("==================================")
print("Date range: ", dts[0], " to ", dts[-1])
print("(lat, lon): (%.2f %.2f)" % (lat, lon))
print("Radius : %.2f" % (args.rad,))
print("==================================")


print("Load matplotlib...")

import matplotlib as mpl
#if args.no_display is False:
#    mpl.use('TkAgg')
#else:
#    mpl.use('Agg')
    #mpl.rc('font', size=20)
 
    
mpl.use('Agg')
# This program plots the AR event
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.patches import Rectangle
import matplotlib.transforms as transforms
from matplotlib.dates import DateFormatter
import matplotlib.ticker as mticker
import cartopy.crs as ccrs
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER

print("done")

print("Create dir: %s" % (args.output_dir,))
Path(args.output_dir).mkdir(parents=True, exist_ok=True)
def findnearest(lat, lon, llat, llon, r=0):

    d = (llat - lat)**2 + (llon-lon)**2

    if r<=0:
        return np.argmin(d)

    else:

        idx = d <= r**2

        idx_list = []

        for i, in_circle in range(len(idx)):

            if in_circle is True:
                idx_list.append(np.unravel_index(i, llat.shape))

        return idx_list
    


def workWrap(*args):

    try:
        result = plot(*args)
    except:
        import traceback
        traceback.print_exc()
        
        result = "ERROR"
    result


def plot(dt, output_filename):

        
    dtstr = dt.strftime("%Y-%m-%d")
    print("Doing date: ", dtstr)


    
    N = 2*args.avg_days+1

    varnames = ["THETA", "SALT"]

    
    data = []
    for varname in varnames:
        data.append(xr.DataArray(
            data = np.zeros((coo.Nz, coo.Ny, coo.Nx)),
            dims=["Z", "Y", "X"],
            coords=dict(
#                lon=(["Y", "X"], coo.grid["XC"]),
#                lat=(["Y", "X"], coo.grid["YC"]),
                lon=(["X"], coo.grid["XC"][0, :]),
                lat=(["Y"], coo.grid["YC"][:, 0]),
                Z=(["Z",], coo.grid["RC"].flatten()),
            ),
        ).rename(varname))
        
    data = xr.merge(data)
    try:
        
        for i in range(N):
             
            _dt_str = (dt + (i - args.avg_days) * pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            _data = dlh.loadDataByDate(_dt_str, msm, **crop_kwargs, datasets=["diag_state",])

            for varname in varnames:
                data[varname] += _data[varname]

        for varname in varnames:
            data[varname] /= N
            
        
        data = data.where( (data.coords["lat"] - lat)**2 + (data.coords["lon"] - lon)**2 <= args.rad**2 ).mean(dim=("X", "Y"), skipna=True) 

    except FileNotFoundError as e:

        print("Some files do not exist.")
        print(str(e))
        
        print("End this job")
        
        return "ERROR"

    print("Data loading complete.")

    ncol = 1
    nrow = 1
    figsize, gridspec_kw = tool_fig_config.calFigParams(
        w = 3,
        h = 5,
        wspace = 1.5,
        hspace = 0.5,
        w_left = 1.0,
        w_right = 1.5,
        h_bottom = 1.0,
        h_top = 1.0,
        ncol = ncol,
        nrow = nrow,
    )

    fig, ax = plt.subplots(
        nrow, ncol,
        figsize=figsize,
        gridspec_kw=gridspec_kw,
        constrained_layout=False,
        squeeze=False,
    )

    coords = data.coords
    print(list(coords.keys()))
    fig.suptitle("%s ; avg days: %d" % ( dt.strftime("%Y-%m-%d"), 1+2*args.avg_days, ))

    _ax = ax[0, 0]; _ax_twin = _ax.twiny()
    _ax.plot(data["THETA"], coords["Z"], label="THETA")
    _ax_twin.plot(data["SALT"], coords["Z"], label="SALT")
    
    print("Output file: ", output_filename)
    fig.savefig(output_filename, dpi=200)
    plt.close(fig)

    return "DONE"    

failed_dates = []
with Pool(processes=args.nproc) as pool:

    input_args = []
    for i, dt in enumerate(dts):
        
        dtstr = dt.strftime("%Y-%m-%d")
        output_filename = "%s/budget_analysis_avg-%d_%s.png" % (args.output_dir, args.avg_days, dtstr)

        if args.overwrite is False and os.path.exists(output_filename):
            print("[%s] File %s already exists. Do not do this job." % (dtstr, output_filename))

        else:
            input_args.append((dt, output_filename))

    
    result = pool.starmap(workWrap, input_args)

