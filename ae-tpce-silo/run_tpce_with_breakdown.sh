#!/bin/bash

exec_path=out-perf.masstree/benchmarks/dbtest
bench=tpce
runtime=30
bench_opts='-w 7,15,0,19,15,9,12,0,20,3 --disable-read-only-snapshots' 

THREADS=( 40 30 20 16 12 8 4 2 1 )
# THREADS=( 1 2 4 8 16 32 48 64 )
# THREADS=( 1 )

for n_threads in ${THREADS[@]}
do
    ntry=5
    while [ ${ntry} -gt 0 ]
    do
        IFS= ret=`./out-perf.masstree/benchmarks/dbtest \
            --bench ${bench} \
            --num-threads ${n_threads} \
            --runtime ${runtime} \
            --retry-aborted-transactions \
            --bench-opts "${bench_opts}"`

        if [ -z $ret ] ; then
            ntry=$(( $ntry - 1 ))
            continue
        fi

        read -ra commit_breakdown <<< `echo $ret | grep 'COMMIT_BREAKDOWN' | sed 's/^COMMIT_BREAKDOWN //g'`
        read -ra abort_breakdown <<< `echo $ret | grep 'ABORT_BREAKDOWN' | sed 's/^ABORT_BREAKDOWN //g'`
        read -ra result <<< `echo $ret | grep 'RESULT' | sed 's/^RESULT //g'`

        break
    done

    output=( "$n_threads" "${commit_breakdown[@]}""${abort_breakdown[@]}""${result[@]}" )
    echo "${output[@]}" | sed 's/ /,/g'
done
