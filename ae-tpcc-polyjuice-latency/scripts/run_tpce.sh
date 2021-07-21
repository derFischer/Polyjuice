#!/bin/bash

exec=out-perf.masstree/benchmarks/dbtest
bench=tpce

runtime=30

#THREADS=( 1 2 4 8 16 32 48 62)
THREADS=( 62 48 32 16 8 4 2 1 )
#THREADS=( 1 )

n_threads=

bench_result="ic3-tpce-s1-rw.csv"
rm -f ${bench_result}
touch ${bench_result}

threads_per_warehouse=

threads_str="n_threads"
throughput_sched_str="throughput"
persist_throughput_str="persist_throughput"
latency_ms_str="latency_ms"
persist_latency_ms_str="persist_latency_ms"
abort_rate_str="abort_rate"
txn_sched_sched_str="txn_sched_time"
for n_threads in ${THREADS[@]}
do
    threads_str=${threads_str}",${n_threads}"

    ntry=5
    ret=
    while [ ${ntry} -gt 0 ]
    do
        ret=`./${exec} \
            --bench ${bench} \
            --num-threads ${n_threads} \
            --bench-opts "-w 2,5,0,6,5,3,64,0,5,10 -s 1" \
            --runtime ${runtime} \
            | grep "RESULT" | sed "s/^RESULT \(.*\)$/\1/g"`
        if [ ${#ret} -ne 0 ]
        then
            IFS=',' read throughput persist_throughput latency_ms persist_latency_ms abort_rate sched_time <<<"${ret}"
            echo "success: throughput: ${throughput}, persist_throughput: ${persist_throughput}, latency_ms: ${latency_ms}, persist_latency_ms: ${persist_latency_ms}, abort_rate: ${abort_rate}, sched_time: ${sched_time}"
            break
        else
            ntry=$((${ntry}-1))
        fi
    done
    if [ ${#ret} -ne 0 ]
    then
        throughput_sched_str="${throughput_sched_str},${throughput}"
        persist_throughput_str="${persist_throughput_str},${persist_throughput}"
        latency_ms_str="${latency_ms_str},${latency_ms}"
        persist_latency_ms_str="${persist_latency_ms_str},${persist_latency_ms}"
        abort_rate_str="${abort_rate_str},${abort_rate}"
        txn_sched_sched_str="${txn_sched_sched_str},${sched_time}"
    else
        throughput_sched_str="${throughput_sched_str},0"
        persist_throughput_str="${persist_throughput_str},0"
        latency_ms_str="${latency_ms_str},0"
        persist_latency_ms_str="${persist_latency_ms_str},0"
        abort_rate_str="${abort_rate_str},0"
        txn_sched_sched_str="${txn_sched_sched_str},0"
    fi
done
echo ${threads_per_warehouse} >>${bench_result}
echo ${threads_str} >>${bench_result}
echo ${throughput_sched_str} >>${bench_result}
echo ${persist_throughput_str} >>${bench_result}
echo ${latency_ms_str} >>${bench_result}
echo ${persist_latency_ms_str} >>${bench_result}
echo ${abort_rate_str} >>${bench_result}
echo ${txn_sched_sched_str} >>${bench_result}
echo "" >>${bench_result}
