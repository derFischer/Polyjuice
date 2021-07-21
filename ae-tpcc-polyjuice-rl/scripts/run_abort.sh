#!/bin/bash

exec=out-perf.masstree/benchmarks/dbtest
bench=microbench

runtime=30

#THREADS=( 1 2 4 8 16 32 48 62 )
THREADS=( 62 )

TXNLEN=( 10 )

RANGE=( 10 20 )
ARATE=( 0 1 2 4 6 8 10 12 14 16 18 20)
#CRATE=( 1 0.5 0.1)

n_threads=

bench_result="ic3-abort-rate.csv"
rm -f ${bench_result}
touch ${bench_result}

threads_per_warehouse=

for range in ${RANGE[@]}
do
    for user_abort_rate in ${ARATE[@]}
    do
        for txnlen in ${TXNLEN[@]}
        do
            read_rate=0
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
                            --disable-snapshot \
                            --parallel-loading \
                            --runtime ${runtime} \
                            --bench-opts "--access-range ${range} --txn-length ${txnlen} --piece-access-recs 4 --user-initial-abort ${user_abort_rate}" \
                            | grep "RESULT" | sed "s/^RESULT \(.*\)$/\1/g"`
                        if [ ${#ret} -ne 0 ]
                        then
                            IFS=',' read throughput p_throughput latency p_latency abort_rate sched_time <<<"${ret}"
                            echo "success: throughput: ${throughput}, abort rate: ${user_abort_rate}"
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
            echo "txn length: ${txnlen}" >>${bench_result}
            echo "access range: ${range}" >>${bench_result}
            echo "user abort rate: ${user_abort_rate}" >>${bench_result}
            # echo "read rate: ${read_rate}" >>${bench_result}
            echo ${threads_str} >>${bench_result}
            echo ${abort_str} >>${bench_result}
            echo ${throughput_str} >>${bench_result}
            echo "" >>${bench_result}
        done
    done
done
