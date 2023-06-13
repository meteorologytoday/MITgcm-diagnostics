#!/bin/bash

avg_days=(
    3
    0
)
archive_dir="output_diag_budgets"
figure_dir="diag_budgets_${avg_days}"

dates=(
    "2018-02-02"  "2018-02-16"
    "2018-01-02"  "2018-01-16"
)

for _avg_days in "${avg_days[@]}"; do

    echo "Doing avg_days = $_avg_days"

    nparams=2
    for (( i=0 ; i < $(( ${#dates[@]} / $nparams )) ; i++ )); do

        beg_date="${dates[$(( i * $nparams + 0 ))]}"
        end_date="${dates[$(( i * $nparams + 1 ))]}"

        input_dir="$archive_dir/$beg_date"
        output_dir="figures/$beg_date/diag_budgets_${_avg_days}"
     
        python3 plot_diag_budgets.py \
            --date-rng $beg_date $end_date \
            --input-dir $input_dir \
            --output-dir $output_dir \
            --avg-days $_avg_days  \
            --varnames heat.dMLTdt heat.G_adv_mflx  heat.G_vdiff      heat.G_hdiff \
                       heat.G_sw   heat.G_lw        heat.G_lat        heat.G_sen   \
                       heat.MLD    heat.dMLDdt      heat.G_ent        heat.G_dil   \
            --ncol 4 \
            --nproc 1 --overwrite
    done

done
