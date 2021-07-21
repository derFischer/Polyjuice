#!/bin/bash

exec=out-perf.masstree/benchmarks/dbtest
bench=tpcc
cores_per_numa=8
mem_per_numa=10

sched_local=1
outstanding_txns=1
workload_mix=45,43,4,4,4
#workload_mix=100,0,0,0,0
runtime=30

THREADS=( 1 2 4 8 16 24 32 40 48 56 62 )
#THREADS=( 1 )

#THREADS_PER_WAREHOUSE=( 64 32 16 8 1 )
THREADS_PER_WAREHOUSE=( 64 )
#THREADS_PER_WAREHOUSE=( 1 )

n_threads=

bench_result="occ-durable-tpcc.csv"
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
                scale_factor=$((${scale_factor_check}+1))
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
                    --txn-flags 1 \
                    --retry-aborted-transactions \
                    --parallel-loading \
                    --num-threads ${n_threads} \
                    --scale-factor ${scale_factor} \
                    --runtime ${runtime} \
                    --bench-opts "--workload-mix ${workload_mix}" \
                    --pin-cpus -m${numa_mem}G \
                    -l occ.log -l /data/sdb1/ycui/occ.log \
                    | grep "RESULT" | sed "s/^RESULT \(.*\)$/\1/g"`
                rm -f occ.log
                rm -f /data/sdb1/ycui/occ.log
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
                txn_sched_sched_str="${txn_sched_sched_str},${sched_time}"
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
