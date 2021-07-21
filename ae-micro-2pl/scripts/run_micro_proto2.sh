#!/bin/bash

exec=out-perf.masstree/benchmarks/dbtest
bench=microbench

runtime=30

THREADS=( 62 )
#TXNLEN=( 1 2 4 8 16 32 48 64 )
TXNLEN=( 5 )
RANGE=( 1000000 100000 10000 1000 100 10 1 )
#CRATE=( 0.1 )

n_threads=

bench_result="micro.csv"
rm -f ${bench_result}
touch ${bench_result}

threads_per_warehouse=

for range in ${RANGE[@]}
do
    for txnlen in ${TXNLEN[@]}
    do
        recs_number=10
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
                        --db-type ndb-proto2 \
                        --num-threads ${n_threads} \
                        --runtime ${runtime} \
                        --bench-opts "--txn-length ${txnlen} --piece-access-recs ${recs_number} --access-range ${range}" \
                        | grep "RESULT" | sed "s/^RESULT \(.*\)$/\1/g"`
                    if [ ${#ret} -ne 0 ]
                    then
                        IFS=',' read throughput p_throughput latency p_latency abort_rate sched_time <<<"${ret}"
                        echo "success: throughput: ${throughput}, abort rate: ${abort_rate}"
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
        echo "#piece: ${txnlen}" >>${bench_result}
        echo "access range: ${range}" >>${bench_result}
        echo "#records / piece: ${recs_number}" >>${bench_result}
        echo ${threads_str} >>${bench_result}
        echo ${abort_str} >>${bench_result}
        echo ${throughput_str} >>${bench_result}
        echo "" >>${bench_result}
    done
done
