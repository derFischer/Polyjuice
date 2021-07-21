#!/bin/bash

exec=out-perf.masstree/benchmarks/dbtest
bench=microbench

runtime=30

THREADS=( 1 2 4 8 16 32 48 62)

TXNLEN=( 1 2 4 16 64 128 256)

n_threads=

bench_result="micro.csv"
rm -f ${bench_result}
touch ${bench_result}

threads_per_warehouse=

for txnlen in ${TXNLEN[@]}
do
    conflict_rate=1
    read_rate=0
    abort_rate=0
    threads_str="n_threads"
    throughput_sched_str="throughput"
    for n_threads in ${THREADS[@]}
    do
	    threads_str=${threads_str}",${n_threads}"
            ntry=5
            ret=
            while [ ${ntry} -gt 0 ]
            do
                ret=`./${exec} \
                    --bench ${bench} \
                    --db-type ndb-proto2 \
                    --num-threads ${n_threads} \
                    --runtime ${runtime} \
                    --bench-opts "--conflict-rate ${conflict_rate} --txn-length ${txnlen} --read-rate ${read_rate} --abort-rate ${abort_rate}" \
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
            else
                throughput_sched_str="${throughput_sched_str},0"
            fi
    done
    echo "txn length: ${txnlen}" >>${bench_result}
    echo "conflit rate: ${conflict_rate}" >>${bench_result}
    echo "abort rate: ${abort_rate}" >>${bench_result}
    echo "read rate: ${read_rate}" >>${bench_result}
    echo ${threads_str} >>${bench_result}
    echo ${throughput_sched_str} >>${bench_result}
    echo "" >>${bench_result}
done
