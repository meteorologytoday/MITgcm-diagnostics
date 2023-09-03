from multiprocessing import Pool
import multiprocessing

import xarray as xr
import argparse
import numpy as np
import os, shutil
import MITgcmDiag.data_loading_helper as dlh
import MITgcmutils as ut
import pandas as pd
import subprocess
import traceback
import re


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

parser.add_argument('--start-datetime', type=str, help='Begin datetime of MITgcm simulation.', required=True)
parser.add_argument('--deltaT', type=float, help='Timestep.', required=True)
parser.add_argument('--dumpfreq', type=float, help='Dumpfreq.', required=True)
parser.add_argument('--avg-hrs', type=int, help='Dumpfreq.', required=True)
parser.add_argument('--skip-hrs', type=int, help='Dumpfreq.', required=True)
parser.add_argument('--date-rng', type=str, nargs=2, help='Date range. If not given then all.', required=True)
parser.add_argument('--root-dir', type=str, help='Input directory that contains all the runs', required=True)
parser.add_argument('--run-label', type=str, help='Label of the run.', required=True)
parser.add_argument('--ensemble-members', type=str, help='Members used to do mean. Ex: "0-9", "2,3,8-10".', required=True)
parser.add_argument('--datasets', type=str, nargs='+', help='Datasets of output filenames that needs to be computed. Empty means all.', default=[])
parser.add_argument('--nproc', type=int, help='Number of subprocesses.', default=1)
parser.add_argument('--output-dir', type=str, help='Output dir', required=True)
parser.add_argument('--naming-rule', type=str, help='Naming rule', default="standard", choices=['standard', 'old'])
args = parser.parse_args()

print(args)

skip_hrs = pd.Timedelta(hours=args.skip_hrs)
avg_hrs  = pd.Timedelta(hours=args.avg_hrs)


beg_dt = pd.Timestamp(args.date_rng[0])
end_dt = pd.Timestamp(args.date_rng[1])



ensemble_members = decomposeRange(args.ensemble_members)


# Generate structure
msms = []
for j, ens in enumerate(ensemble_members):
    data_dir = genRunDirName(args.run_label, ens, root_dir=args.root_dir, naming_rule=args.naming_rule)
    grid_dir = data_dir
    msms.append(
        dlh.MITgcmSimMetadata(
            args.start_datetime,
            args.deltaT,
            args.dumpfreq,
            data_dir,
            grid_dir,        
        )
    )

if not os.path.exists(args.output_dir):
    print("Making output direcory: %s" % (args.output_dir,))
    os.makedirs(args.output_dir)

ref_msm = msms[0]
def doWork(dt, dataset):
  

    dt_str = dt.strftime("%Y-%m-%d_%H")
    result = dict(status="OK", dt=dt, dataset=dataset)
    print("Doing datetime %s of dataset %s" % (dt_str, dataset))
   
    beg_time = dt 
    end_time = dt + avg_hrs 

    # Load data one by one

    try:
        bundle    = None
        data_mean = None
        data_std = None

        N = len(ensemble_members)

        for i, ens in enumerate(ensemble_members):
            _bundle = dlh.raw_loadAveragedDataByDateRange(
                beg_time,
                end_time,
                msms[i],
                dataset=dataset,
                inclusive="right",
            )

            if i == 0:
                bundle = _bundle
                data_mean = bundle['data']
            else:
                data_mean   += _bundle['data']

        data_mean /= N


        for i, ens in enumerate(ensemble_members):
            _bundle = dlh.raw_loadAveragedDataByDateRange(
                beg_time,
                end_time,
                msms[i],
                dataset=dataset,
                inclusive="right",
            )

            if i == 0:
                data_std = (_bundle['data'] - data_mean)**2
            else:
                data_std += (_bundle['data'] - data_mean)**2

        data_std = ( data_std / (N-1) )**0.5

        del bundle['data']
        bundle['data_mean'] = data_mean     
        bundle['data_std'] = data_std

        # Save data
        for stat, prefix in dict(mean="", std="std_").items():

            output_dataset = "%s%s" % (prefix, dataset)
            output_dataset_full = "%s/%s" % (args.output_dir, output_dataset)
            print("[%s] Output: %s" % (dt_str, output_dataset_full,))
                
            print(bundle['metadata']['fldlist'])
            ut.mds.wrmds(
                output_dataset_full,
                bundle['data_%s' % (stat,)],
                itr=bundle['iters'],
                ndims=bundle['metadata']['ndims'],
                fields=bundle['metadata']['fldlist'],
            )

    except Exception as e:
        traceback.print_exc()
        print(e)
        result['status'] = "ERROR"        

    return result



with Pool(processes=args.nproc) as pool:

    input_args = []
    dts = pd.date_range(beg_dt, end_dt, freq=skip_hrs, inclusive="left")
    
    for i, dt in enumerate(dts):
            
        dt_str = dt.strftime("%Y-%m-%d_%H")
        for j, dataset in enumerate(args.datasets):

            output_filenames = []
            for stat, prefix in dict(mean="", std="std_").items():
                _filenames = dlh.genMITgcmFilenames("%s%s" % (prefix, dataset), dt + avg_hrs, ref_msm)
                output_filenames.extend(_filenames)
            

            all_exists = True
            for output_filename in output_filenames:
                output_filename_full = os.path.join(args.output_dir, output_filename)
                print(output_filename_full)
                if not os.path.exists(output_filename_full):
                    all_exists = False

            if all_exists:
                print("Output file for dataset %s on time %s already exists. Skip it." % (dataset, dt_str, ))
                continue
            
            input_args.append((dt, dataset))

    
    results = pool.starmap(doWork, input_args)

    for i, result in enumerate(results):
        if result['status'] != 'OK':
            print('[%d] Failed to generate output dataset %s of datetime %s.' % (
                i,
                result['dataset'],
                result['dt'].strftime("%Y-%m-%d_%H"),
            ))

