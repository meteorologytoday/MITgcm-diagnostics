from multiprocessing import Pool
import multiprocessing

import xarray as xr
import argparse
import numpy as np
import os, shutil
import WRFDiag.wrf_load_helper as wlh
import pandas as pd
import subprocess
import re
import traceback


wlh.engine = "netcdf4"

def genRunDirName(run_label, ens, root_dir="", naming_rule="standard"):
    
    if naming_rule == "standard":
        fmt = "%se%02d"
    elif naming_rule == "old":
        fmt = "%s_ens%02d"
    else:
        raise Exception("Unknown `naming_rule` : %s" % (naming_rule, ) )
 
    subdir = fmt % (run_label, ens)
    
    if root_dir == "":
        p = subdir
    else:
        p = os.path.join(root_dir, subdir)
        
    return p

def decomposeRange(s):
   
    s = s.replace(' ','') 
    r = re.findall(r'([0-9]+)(?:-([0-9])+)?,?', s)

    output = []
    for i, (x1, x2) in enumerate(r):
        
        x1 = int(x1)
        if x2 == '':
            output.append(x1)
            
        else:
            x2 = int(x2)

            output.extend(list(range(x1,x2+1))) 

    return output


parser = argparse.ArgumentParser(
                    prog = 'mk_ens_mean',
                    description = 'Make mean of ensemble runs.',
)

parser.add_argument('--date-rng', type=str, nargs=2, help='Date range. If not given then all.', default=None)
parser.add_argument('--skip-hrs', type=int, help='The skip in hours to do the next diag.', required=True)
parser.add_argument('--avg-hrs', type=int, help='The length of time to do the average in hours.', default=np.nan)
parser.add_argument('--root-dir', type=str, help='Input directory that contains all the runs', required=True)
parser.add_argument('--run-label', type=str, help='Label of the run.', required=True)
parser.add_argument('--ensemble-members', type=str, help='Members used to do mean. Ex: "0-9", "2,3,8-10".', required=True)
parser.add_argument('--varnames', type=str, nargs='*', help='Varnames needed to do ensemble mean. Empty means all.', default=[])
parser.add_argument('--nproc', type=int, help='Number of subprocesses.', default=1)
parser.add_argument('--output-dir', type=str, help='Output dir', required=True)
parser.add_argument('--wrf-prefix', type=str, help='Output dir', default='wrfout_d01_')
parser.add_argument('--naming-rule', type=str, help='Naming rule', default="standard", choices=['standard', 'old'])
args = parser.parse_args()

print(args)



date_rng = args.date_rng

skip_hrs = pd.Timedelta(hours=args.skip_hrs)
avg_hrs  = pd.Timedelta(hours=args.avg_hrs)

ensemble_members = decomposeRange(args.ensemble_members)

input_dirs = []
for j, ens in enumerate(ensemble_members):
    input_dirs.append(os.path.join(args.root_dir, genRunDirName(args.run_label, ens, naming_rule=args.naming_rule)))
    
if not os.path.exists(args.output_dir):
    print("Making output direcory: %s" % (args.output_dir,))
    os.makedirs(args.output_dir)


def doWork(time_rng, output_filename_mean, output_filename_std):
  
    beg_time_str = time_rng[0].strftime("%Y-%m-%d_%H") 
    end_time_str = time_rng[1].strftime("%Y-%m-%d_%H") 
    print("Running time range: [%s , %s]" % (
        beg_time_str,
        end_time_str,
    ))

    result = dict(status="OK", time_rng=time_rng)
    try:

        ds_mean = None
        ds_sqr  = None
        stat_varnames = []
        shared_attrs = None
        N = len(input_dirs)

        coords = None

        # In the following code, I compute the mean and
        # std separately. This is because the fomula
        # var = <x^2> - <x>^2 can be negative due to numerical
        # error. Therefore, I need the formula
        # var = N^(-1) \Sigma (x_i - <x>)^2 
        # to ensure the sign is positive

        for i, input_dir in enumerate(input_dirs):
                
            _ds = wlh.loadWRFDataFromDir(input_dir, prefix=args.wrf_prefix, time_rng=time_rng, verbose=True, avg=True)
            if i == 0:

                # Loop through variable to know which to work on
                if len(args.varnames) == 0:
                    all_varnames = list(_ds.keys())
                    for varname in all_varnames:
                        if _ds[varname].dtype in [ np.float64, np.float32 ]:
                            stat_varnames.append(varname)

                else:
                    stat_varnames.extend(args.varnames)

                shared_attrs = _ds.attrs
                #coords = [ _ds[varname] for varname in ["XLAT", "XLONG", ] ]
            
            else:
                # Test this first
                non_exist_varnames = []
                for k in stat_varnames:
                    if not (k in _ds):
                        non_exist_varnames.append(k)

                if len(non_exist_varnames) != 0:
                    raise Exception("Error: The variable(s) %s do not exist!" % ( ", ".join(non_exist_varnames),))

            # Extract varialbes
            __ds = xr.merge([ _ds[varname] for varname in stat_varnames ])

            if i == 0:
                ds_mean = __ds

            else:
                ds_mean += __ds


        ds_mean /= N 
 
        for i, input_dir in enumerate(input_dirs):
                
            _ds = wlh.loadWRFDataFromDir(input_dir, prefix=args.wrf_prefix, time_rng=time_rng, verbose=True, avg=True)
            # No need to test variables anymore because they have been all tested.
            # Extract varialbes
            __ds = xr.merge([ _ds[varname] for varname in stat_varnames ])

            if i == 0:
                ds_std  = (__ds - ds_mean)**2

            else:
                ds_std  += (__ds - ds_mean)**2


        ds_std  /= N
        ds_std = ( ds_std * ( N / ( N - 1 ) ) )**0.5

        #ds_mean = xr.merge([ds_mean, *coords]).assign_attrs(**shared_attrs)
        #ds_std = xr.merge([ds_std, *coords]).assign_attrs(**shared_attrs)
        ds_mean = ds_mean.assign_attrs(**shared_attrs)
        ds_std  = ds_std.assign_attrs(**shared_attrs)


        print("Output files of time %s to %s" % (beg_time_str, end_time_str,))
        ds_mean.to_netcdf(output_filename_mean, unlimited_dims='time')
        ds_std.to_netcdf(output_filename_std, unlimited_dims='time')
   
    except Exception as e:
        
        result['status'] = "ERROR"        
        traceback.print_stack()
        traceback.print_exc()

    return result



with Pool(processes=args.nproc) as pool:

    input_args = []
    dts = pd.date_range(args.date_rng[0], args.date_rng[1], freq=skip_hrs, inclusive="left")
    
    fmt = "%Y-%m-%d_%H:%M:%S"
    
    for dt in dts:

        beg_time = dt
        end_time = dt + avg_hrs

        dtstr = dt.strftime(fmt)
        output_filenames = dict(
            output_filename_mean = os.path.join(args.output_dir, "%s%s" % (args.wrf_prefix, dtstr, )),
            output_filename_std  = os.path.join(args.output_dir, "std_%s%s"  % (args.wrf_prefix, dtstr, )),
        )

        all_exists = True
        for _, output_filename in output_filenames.items():
            all_exists = all_exists and os.path.exists(output_filename)


        if all_exists:
            print("Output files of date %s all exists. Skip it." % (dtstr,))
            continue

        input_args.append(([beg_time, end_time], output_filenames['output_filename_mean'], output_filenames['output_filename_std']))


    print("Ready to distribute jobs... ")

    results = pool.starmap(doWork, input_args)

    for i, result in enumerate(results):

        if result['status'] != 'OK':
            
            print('[%d] Failed to generate output files for time_rng = [ %s, %s ].' % (
                i,
                result['time_rng'][0].strftime("%Y-%m-%d_%H"),
                result['time_rng'][1].strftime("%Y-%m-%d_%H"),
            ))

