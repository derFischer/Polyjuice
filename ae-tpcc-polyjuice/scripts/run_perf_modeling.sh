#!/bin/bash

exec=out-perf.masstree/benchmarks/dbtest
bench=tpcc
cores_per_numa=8
mem_per_numa=10

sched_local=1
outstanding_txns=1
workload_mix=100,0,0,0,0
runtime=30

THREADS=( 1 2 4 8 16 24 32 40 48 56 62)
#THREADS=( 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62)

THREADS_PER_WAREHOUSE=( 64 )

n_threads=

bench_result="perf.csv"
rm -f ${bench_result}
touch ${bench_result}

threads_per_warehouse=

for threads_per_warehouse in ${THREADS_PER_WAREHOUSE[@]}
do
    scale_factor_str="scale_factor"
    threads_str="n_threads"
    throughput_sched_str="throughput"
    latency_sched_str="latency"
    running_time_sched_str="running_time"
    txn_init_sched_str="txn_init_time"
    txn_free_sched_str="txn_free_time"
    txn_sched_sched_str="txn_sched_time"
    for n_threads in ${THREADS[@]}
    do
        n_numas=$((${n_threads}/${cores_per_numa}))
        mod_buf=$((${n_threads}%${cores_per_numa}))
        if [ ${mod_buf} -ne 0 ]
        then
            n_numas=$((${n_numas}+1))
        fi
        numa_mem=$((${mem_per_numa}*${n_numas}))
        scale_factor=1
        if [ ${n_threads} -gt ${threads_per_warehouse} ]
        then
            scale_factor_check=$((${n_threads}/${threads_per_warehouse}))
            scale_factor_mul=$((${threads_per_warehouse}*${scale_factor_check}))
            if [ ${scale_factor_mul} -eq ${n_threads} ]
            then
                scale_factor=${scale_factor_check}
            else
                scale_factor=0
            fi
        fi


        if [ ${scale_factor} -ne 0 ]
        then
            scale_factor_str=${scale_factor_str}",${scale_factor}"
            threads_str=${threads_str}",${n_threads}"

            ntry=5
            ret=
            while [ ${ntry} -gt 0 ]
            do
                ret=`./${exec} \
                    --bench ${bench} \
                    --db-type ndb-ic3 \
                    --txn-flags 1 \
                    --retry-aborted-transactions \
                    --num-threads ${n_threads} \
                    --runtime ${runtime} \
  		    --parallel-loading \
 		    --bench-opts "--workload-mix ${workload_mix}" \
                    | grep "RESULT" | sed "s/^RESULT \(.*\)$/\1/g"`
                if [ ${#ret} -ne 0 ]
                then
                    IFS=',' read throughput latency running_time init_time free_time sched_time <<<"${ret}"
                    echo "success: throughput: ${throughput}, latency: ${latency}, running_time: ${running_time}, init_time: ${init_time}, free_time: ${free_time}, sched_time: ${sched_time}"
                    break
                else
                    ntry=$((${ntry}-1))
                fi
            done
            if [ ${#ret} -ne 0 ]
            then
                throughput_sched_str="${throughput_sched_str},${throughput}"
                latency_sched_str="${latency_sched_str},${latency}"
                running_time_sched_str="${running_time_sched_str},${running_time}"
                txn_init_sched_str="${txn_init_sched_str},${init_time}"
                txn_free_sched_str="${txn_free_sched_str},${free_time}"
                txn_sched_sc66hed_str="${txn_sched_sched_str},${sched_time}"
            else
                throughput_sched_str="${throughput_sched_str},0"
                latency_sched_str="${latency_sched_str},0"
                running_time_sched_str="${running_time_sched_str},0"
                txn_init_sched_str="${txn_init_sched_str},0"
                txn_free_sched_str="${txn_free_sched_str},0"
                txn_sched_sched_str="${txn_sched_sched_str},0"
            fi
        fi
    done
    echo ${threads_per_warehouse} >>${bench_result}
    echo ${scale_factor_str} >>${bench_result}
    echo ${threads_str} >>${bench_result}
    echo ${throughput_sched_str} >>${bench_result}
    echo ${latency_sched_str} >>${bench_result}
    echo ${running_time_sched_str} >>${bench_result}
    echo ${txn_init_sched_str} >>${bench_result}
    echo ${txn_free_sched_str} >>${bench_result}
    echo ${txn_sched_sched_str} >>${bench_result}
    echo "" >>${bench_result}
done
