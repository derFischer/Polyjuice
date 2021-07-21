#!/bin/bash

exec=out-perf.masstree/benchmarks/dbtest
bench=tpcc

workload_mix=45,43,4,4,4
runtime=30

#THREADS=( 1 2 4 8 16 32 48 62 )
THREADS=( 62 )
#TXNLEN=( 1 2 4 8 16 32 48 64 )
SCALE=( 62 32 16 8 4 2 1 )

n_threads=

bench_result="tpcc.csv"

rm -f ${bench_result}
touch ${bench_result}

for scale in ${SCALE[@]}
do
    threads_str="n_threads"
    throughput_str="throughput"
    abort_str="abort rate"
    for n_threads in ${THREADS[@]}
    do
	    threads_str=${threads_str}",${n_threads}"
            ntry=5
            ret=
            while [ ${ntry} -gt 0 ]
            do
                ret=`./${exec} \
                    --bench ${bench} \
                    --db-type ndb-ic3 \
                    --num-threads ${n_threads} \
                    --runtime ${runtime} \
                    --scale-factor ${scale} \
            		--parallel-loading \
                    --bench-opts "--workload-mix ${workload_mix}" \
                    | grep "RESULT" | sed "s/^RESULT \(.*\)$/\1/g"`
                if [ ${#ret} -ne 0 ]
                then
                    IFS=',' read throughput p_throughput latency p_latency abort_rate sched_time <<<"${ret}"
                    echo "TPCC Result: thread: ${n_threads}, scale factor: ${scale} throughput: ${throughput}, abort rate: ${abort_rate}"
                    break
                else
                    ntry=$((${ntry}-1))
                fi
            done
            if [ ${#ret} -ne 0 ]
            then
                throughput_str="${throughput_str},${throughput}"
                abort_str="${abort_str},${abort_rate}"
            else
                throughput_str="${throughput_str},0"
                abort_str="${abort_str},0"
            fi
    done
    echo "scale factor: ${scale}" >>${bench_result}
    echo ${threads_str} >>${bench_result}
    echo ${abort_str} >>${bench_result}
    echo ${throughput_str} >>${bench_result}
    echo "" >>${bench_result}
done
