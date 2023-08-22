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
parser.add_argument('--naming-rule', type=str, help='Naming rule', default="standard", choices=['standard', 'old'])
args = parser.parse_args()

print(args)



date_rng = args.date_rng

skip_hrs = pd.Timedelta(hours=args.skip_hrs)
avg_hrs  = pd.Timedelta(hours=args.avg_hrs)
dts = pd.date_range(args.date_rng[0], args.date_rng[1], freq=skip_hrs, inclusive="left")


if date_rng is not None:

    beg_dt = pd.Timestamp(args.date_rng[0])
    end_dt = pd.Timestamp(args.date_rng[1])
    
    date_rng = [beg_dt, end_dt]    


ensemble_members = decomposeRange(args.ensemble_members)

input_dirs = []
for j, ens in enumerate(ensemble_members):
    input_dirs.append(genRunDirName(args.run_label, ens, naming_rule=args.naming_rule))
    

# List all possible files from one of the ensemble members
test_dir = os.path.join(args.root_dir, input_dirs[0])
wrf_filenames = wlh.listWRFOutputFiles(test_dir, time_rng=date_rng)

print("========== List of found wrf files in %s =========" % (test_dir,))
for i, wrf_filename in enumerate(wrf_filenames):
    print("[%2d] %s" % (i, wrf_filename,))


if len(args.varnames) == 0:
    varnames_cmd = ""

else:
    varnames_cmd = "-v %s" % ( ",".join(args.varnames), )


if not os.path.exists(args.output_dir):
    print("Making output direcory: %s" % (args.output_dir,))
    os.makedirs(args.output_dir)


work_batches = [
    [],  # mean
    [],  # avgsqr
]


for i, wrf_filename in enumerate(wrf_filenames):
    output_filename = os.path.join(args.output_dir, "%s" % (wrf_filename, ))
    input_filenames = [os.path.join(input_dir, wrf_filename) for input_dir in input_dirs]
    cmd = ["ncea", varnames_cmd, "-O", "-p",  args.root_dir, *input_filenames, output_filename]
    cmd = " ".join(cmd)
    print(cmd)
    work_batches[0].append([output_filename, cmd])


for i, wrf_filename in enumerate(wrf_filenames):
    output_filename = os.path.join(args.output_dir, "avgsqr_%s" % (wrf_filename, ))
    input_filenames = [os.path.join(input_dir, wrf_filename) for input_dir in input_dirs]
    cmd = ["nces", varnames_cmd, "-O", "-p",  args.root_dir, "-y avgsqr", *input_filenames, output_filename]
    cmd = " ".join(cmd)
    print(cmd)
    work_batches[1].append([output_filename, cmd])




def doWork(work_id, cmd):
   
    print("Running command: ", cmd) 
    r = subprocess.run(cmd, capture_output=True, shell=True)

    if r.returncode != 0:
        print(r)


    return work_id, r.returncode 



with Pool(processes=args.nproc) as pool:

    input_args = []
    
    for work_batch in work_batches:
        
        for work_id, (output_filename, cmd) in enumerate(work_batch):

            if os.path.exists(output_filename):
                print("Output file: %s already exists. Skip it." % (output_filename,))
                continue

            input_args.append((work_id, cmd))

        results = pool.starmap(doWork, input_args)

        for work_id, returncode in enumerate(results):

            if returncode != 0:
                print('[%d] Failed to generate output file %s.' % (work_id, work_batch[work_id][1]))

