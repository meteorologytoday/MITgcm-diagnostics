#!/bin/bash


diag_dir="output_diag_budgets"
archive_dir="/home/t2hsu/projects/diag_skrips/data"

mitgcm_deltaT=60.0
nproc=16


dates=(
    "2018-01-02"  "2018-01-16"
)
#    "2018-02-02"  "2018-02-16"
#)



#for tracer in salt heat ; do

nparams=2
for (( i=0 ; i < $(( ${#dates[@]} / $nparams )) ; i++ )); do

    beg_date="${dates[$(( i * $nparams + 0 ))]}"
    end_date="${dates[$(( i * $nparams + 1 ))]}"
    mitgcm_beg_date=$beg_date


    sel_date="2018-01-03"
    data_dir=$archive_dir/$beg_date
    output_dir=$diag_dir/$beg_date

    for tracer in heat ; do

        echo "Now diagnose tracer: $tracer"
       
         
        _output_dir=$output_dir/$tracer

        python3 test_${tracer}_budgets.py \
            --data-dir $data_dir \
            --grid-dir $data_dir \
            --mitgcm-beg-date $mitgcm_beg_date \
            --mitgcm-deltaT $mitgcm_deltaT \
            --beg-date $beg_date \
            --sel-date $sel_date \
            --lat-rng 20 52 \
            --lon-rng 180 244 \

    done

done
