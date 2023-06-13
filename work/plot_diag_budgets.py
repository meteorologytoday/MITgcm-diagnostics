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


parser = argparse.ArgumentParser(
                    prog = 'plot_skill',
                    description = 'Plot prediction skill of GFS on AR.',
)

parser.add_argument('--date-rng', type=str, nargs=2, help='Date range.', required=True)
parser.add_argument('--avg-days', type=int, help='How many days before and after to average.', required=True)
parser.add_argument('--input-dir', type=str, help='Input dir', default="output_diag_budgets")
parser.add_argument('--output-dir', type=str, help='Output dir', default="output_figure")
parser.add_argument('--nproc', type=int, help='Number of processors.', default=1)
parser.add_argument('--lat', type=float, nargs=2, help='Latitudes in degree', default=[30, 52])
parser.add_argument('--lon', type=float, nargs=2, help='Longitudes in degree', default=[360-180, 360-144])
parser.add_argument('--overwrite', action="store_true")
parser.add_argument('--varnames', type=str, nargs="+", help='varnames', required=True)
parser.add_argument('--ncol', type=int, default=0)
args = parser.parse_args()
print(args)


dts = pd.date_range(args.date_rng[0], args.date_rng[1], freq="D", inclusive="both")

args.lon = np.array(args.lon) % 360.0

lat_n, lat_s = np.amax(args.lat), np.amin(args.lat)
lon_w, lon_e = np.amin(args.lon), np.amax(args.lon)


print("==================================")
print("Date range: ", dts[0], " to ", dts[-1])
print("Latitude  box: %.2f %.2f" % (lat_s, lat_n))
print("Longitude box: %.2f %.2f" % (lon_w, lon_e))
print("==================================")







heat_G_levs  = np.linspace(-1, 1, 51) * 1.0
heat_G_ticks = np.linspace(-1, 1, 11) * 1.0
heat_G_cmap  = "bwr"
heat_G_factor = 1e-6
heat_G_unit = "$ 1 \\times 10^{-6} \\, \\mathrm{K} / \\mathrm{s} $ ]"

salt_G_levs  = np.linspace(-1, 1, 51) * 1.0
salt_G_ticks = np.linspace(-1, 1, 11) * 1.0
salt_G_cmap  = "bwr"
salt_G_factor = 1e-6
salt_G_unit = "$ 1 \\times 10^{-6} \\, \\mathrm{PSU} / \\mathrm{s} $ ]"



dMLDdt_levs  = np.linspace(-1, 1, 51) * 10
dMLDdt_ticks  = np.linspace(-1, 1, 21) * 10

MLD_levs  = np.linspace(0,  1, 51) * 100
MLD_ticks  = np.linspace(0, 1, 11) * 100

plot_infos = dict(

    heat = {

        "dMLTdt" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor= heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{ttl}$",
        ),

        "G_adv_mflx" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor= heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{adv} + \\dot{\\overline{\\Theta}}_\\mathrm{mflx}$",
        ),

        "G_sw_nsw" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{sw} + \\dot{\\overline{\\Theta}}_\\mathrm{nsw}$",
        ),


        "G_nsw" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{sfc}$",
        ),


        "G_sw" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{sw}$",
        ),

        "G_lw" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{lw}$",
        ),

        "G_lat" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{lat}$",
        ),

        "G_sen" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{sen}$",
        ),

        "G_adv" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{adv}$",
        ),

        "G_hdiff" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{hdiff}$",
        ),


        "G_vdiff" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{vdiff}$",
        ),

        "G_ent" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{ent}$",
        ),

        "G_vdiff_ent" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{vdiff} + \\dot{\\overline{\\Theta}}_\\mathrm{ent}$",
        ),


        "G_hdiff" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{hdiff}$",
        ),

        "G_dil" : dict(
            levs  = heat_G_levs,
            ticks = heat_G_ticks,
            cmap  = heat_G_cmap,
            unit  = heat_G_unit,
            factor = heat_G_factor,
            label = "$\\dot{\\overline{\\Theta}}_\\mathrm{dilu}$",
        ),


        "MLD" : dict(
            levs  = MLD_levs,
            ticks = MLD_ticks,
            cmap  = "GnBu",
            unit  = "$ \\mathrm{m} $",
            label = "$ h $",
        ),

        "dMLDdt" : dict(
            levs  = dMLDdt_levs,
            ticks = dMLDdt_ticks,
            factor = 1e-5,
            cmap  = "bwr_r",
            unit  = "$ 1 \\times 10^{-5} \\mathrm{m} / \\mathrm{s} $",
            label = "$ \\partial h / \\partial t $",
        ),

    }, 

    salt = {

        "dMLSdt" : dict(
            levs  = salt_G_levs,
            ticks = salt_G_ticks,
            cmap  = salt_G_cmap,
            unit  = salt_G_unit,
            factor= salt_G_factor,
            label = "$\\dot{\\overline{S}}_\\mathrm{ttl}$",
        ),

        "G_sfc" : dict(
            levs  = salt_G_levs,
            ticks = salt_G_ticks,
            unit  = salt_G_unit,
            cmap  = salt_G_cmap,
            factor= salt_G_factor,
            label = "$\\dot{\\overline{S}}_\\mathrm{sfc}$",
        ),

        "G_adv_fwf" : dict(
            levs  = salt_G_levs,
            ticks = salt_G_ticks,
            unit  = salt_G_unit,
            cmap  = salt_G_cmap,
            factor= salt_G_factor,
            label = "$\\dot{\\overline{S}}_\\mathrm{adv} + \\dot{\\overline{S}}_\\mathrm{fwf}$",
        ),

        "G_adv" : dict(
            levs  = salt_G_levs,
            ticks = salt_G_ticks,
            unit  = salt_G_unit,
            cmap  = salt_G_cmap,
            factor= salt_G_factor,
            label = "$\\dot{\\overline{S}}_\\mathrm{adv}$",
        ),

        "G_vdiff" : dict(
            levs  = salt_G_levs,
            ticks = salt_G_ticks,
            unit  = salt_G_unit,
            cmap  = salt_G_cmap,
            factor= salt_G_factor,
            label = "$\\dot{\\overline{S}}_\\mathrm{vdiff}$",
        ),

        "G_ent" : dict(
            levs  = salt_G_levs,
            ticks = salt_G_ticks,
            unit  = salt_G_unit,
            cmap  = salt_G_cmap,
            factor= salt_G_factor,
            label = "$\\dot{\\overline{S}}_\\mathrm{ent}$",
        ),

        "G_vdiff_ent" : dict(
            levs  = salt_G_levs,
            ticks = salt_G_ticks,
            unit  = salt_G_unit,
            cmap  = salt_G_cmap,
            factor= salt_G_factor,
            label = "$\\dot{\\overline{S}}_\\mathrm{vdiff} + \\dot{\\overline{S}}_\\mathrm{ent}$",
        ),


        "G_fwf" : dict(
            levs  = salt_G_levs,
            ticks = salt_G_ticks,
            unit  = salt_G_unit,
            cmap  = salt_G_cmap,
            factor= salt_G_factor,
            label = "$\\dot{\\overline{S}}_\\mathrm{fwf}$",
        ),


    }, 

)


# Detecting what files to load

loaded_category = []

ncol = args.ncol

if ncol <= 0: # implies only one row
    ncol = len(args.varnames)

nrow = int( np.ceil( len(args.varnames) / ncol ) )




plotted_arragement = np.zeros((nrow, ncol), dtype=object)

# Pad the configuration with BLANK
for i in range(plotted_arragement.size - len(args.varnames)):
    args.varnames.append("BLANK") 

for i, long_varname in enumerate(args.varnames):
    
    if long_varname == "BLANK":
        category, varname = "BLANK.BLANK".split(".")
   
    else: 
        category, varname = long_varname.split(".")
        
        if category not in loaded_category:
            loaded_category.append(category)
        
        if varname not in plot_infos[category]:
            print(plot_infos[category].keys())
            raise Exception("Varname %s.%s does not have plot info specified. Please update it. " % (category, varname,))

                
    plotted_arragement[np.unravel_index(i, (nrow, ncol))] = (category, varname)

print("==================================")
print("Category detected: ")
for i, category in enumerate(loaded_category):
    print("[%d] %s" % (i, category,))

print("==================================")
print("Figure arrangement (nrow, ncol) = (%d, %d):" % (nrow, ncol))
for j in range(nrow):
    for i in range(ncol):
        print("(%s.%s) \t" % plotted_arragement[j, i], end="")

    print()

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

    _data = []
    
    # filenames


    _data = {}
    _data_mean = {}
    for category in loaded_category:
    
        needed_filenames = []

        print("Loading category: ", category)
        needed_filenames.extend([
            "%s/%s/%s_budget_analysis_%s.nc" % (
                args.input_dir,
                category,
                category,
                (dt + (i - args.avg_days) * pd.Timedelta(days=1)).strftime("%Y-%m-%d")) for i in range(2*args.avg_days+1)
        ])

        print("List of loaded files: ")
        for i, filename in enumerate(needed_filenames):
            print("[%2d] %s" % (i, filename,))

        try:
            __data = xr.open_mfdataset(needed_filenames)

            if category == "heat":
                __data = xr.merge([
                    __data,
                    __data["G_diffz"].rename("G_vdiff"),
                    (__data["G_sw"] + __data["G_nsw"]).rename("G_sw_nsw"),
                    (__data["G_diffz"] + __data["G_ent"]).rename("G_vdiff_ent"),
                    (__data["G_diffx"] + __data["G_diffy"]).rename("G_hdiff"),
                ])

            if category == "salt":
                __data = xr.merge([
                    __data,
                    (__data["G_vdiff"] + __data["G_ent"]).rename("G_vdiff_ent"),
                ])


            __data_mean = __data.mean(dim="time")

            _data[category] = __data
            _data_mean[category] = __data_mean

        except FileNotFoundError as e:

            print("Some files do not exist.")
            print(str(e))
            
            print("End this job")
            
            return "ERROR"

    print("Data loading complete.")
    cent_lon = 180.0

    plot_lon_l = 360 - 180
    plot_lon_r = 360 - 116
    plot_lat_b = 30.0
    plot_lat_t = 52.0

    proj = ccrs.PlateCarree(central_longitude=cent_lon)
    proj_norm = ccrs.PlateCarree()

    figsize, gridspec_kw = tool_fig_config.calFigParams(
        w = 6,
        h = 3,
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
        subplot_kw=dict(projection=proj, aspect="auto"),
        gridspec_kw=gridspec_kw,
        constrained_layout=False,
        squeeze=False,
    )

    coords = __data.coords
    #cmap = cm.get_cmap("bwr")
    #cmap.set_over("green")
    #cmap.set_under("yellow")

    fig.suptitle("%s ; avg days: %d" % ( dt.strftime("%Y-%m-%d"), 1+2*args.avg_days, ))


    for (j, i), (category, varname) in np.ndenumerate(plotted_arragement):
        
        _ax = ax[j, i]
    
        if category == "BLANK":
            _ax.remove()
            continue
        
        dm = _data_mean[category][varname]
        plot_info = plot_infos[category][varname]

        if "factor" in plot_info:
            dm /= plot_info["factor"]


        mappable = _ax.contourf(coords["lon"], coords["lat"], dm, levels=plot_info["levs"], transform=proj_norm, extend="both", cmap=plot_info["cmap"])

        cax = tool_fig_config.addAxesNextToAxes(fig, _ax, "right", thickness=0.03, spacing=0.05)
        cb = plt.colorbar(mappable, cax=cax, ticks=plot_info["ticks"], orientation="vertical", pad=0.0)
        cb.ax.set_ylabel(" %s [ %s ]" % (plot_info["label"], plot_info["unit"]))

    
        _ax.set_title(plot_info["label"])

        """
        _ax.quiver(coords["lon"], coords["lat"], _data.u10.to_numpy(), _data.v10.to_numpy(), scale=200, transform=proj_norm)

        cs = _ax.contourf(coords["lon"], coords["lat"], _data['map'], colors='none', levels=[0, 0.5, np.inf], hatches=[None, "."], transform=proj_norm)

        # Remove the contour lines for hatches 
        for _, collection in enumerate(cs.collections):
            collection.set_edgecolor("red")
        """


        _ax.set_global()
        #__ax.gridlines()
        _ax.coastlines()#color='gray')
        _ax.set_extent([plot_lon_l, plot_lon_r, plot_lat_b, plot_lat_t], crs=proj_norm)

        gl = _ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                          linewidth=1, color='gray', alpha=0.5, linestyle='--')

        gl.xlabels_top   = False
        gl.ylabels_right = False

        #gl.xlocator = mticker.FixedLocator(np.arange(-180, 181, 30))
        #gl.xlocator = mticker.FixedLocator([120, 150, 180, -150, -120])#np.arange(-180, 181, 30))
        #gl.ylocator = mticker.FixedLocator([10, 20, 30, 40, 50])
        
        gl.xformatter = LONGITUDE_FORMATTER
        gl.yformatter = LATITUDE_FORMATTER
        gl.xlabel_style = {'size': 10, 'color': 'black'}
        gl.ylabel_style = {'size': 10, 'color': 'black'}


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

