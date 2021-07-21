#!/bin/bash

usage() {
    echo "usage: ./test.sh <N> <nw>"
    exit -1
}

name=test_result_tpcc_occ_scalability_8wh.txt

if [ "$#" -ne 2 ]; then
    usage
elif ! [[ $1 =~ ^[0-9]+$ ]] ; then
    echo "$1 is not a number"
    usage
elif ! [[ $2 =~ ^[0-9]+$ ]] ; then
    echo "$2 is not a number"
    usage
fi

echo "TEST const 1 warehouse" > $name


# for num_thread in 1 2 4 8 12 16 24 48 
# do
#     echo "------------------------ $num_thread thread  1 wh--------------------------"
#     echo "------------------------ $num_thread thread  1 wh--------------------------" >> $name
#     for ((i=0;i<$1;i++));
#     do
#         ./out-perf.masstree/benchmarks/dbtest --bench tpcc --parallel-loading --retry-aborted-transactions --bench-opt "--workload-mix 45,43,4,4,4" --db-type ndb-proto2  --backoff-aborted-transactions --runtime 30 --num-threads $num_thread --scale-factor 1 >> $name
#     done
# done


for num_thread in 1 2 4 8 12 16 24 48 
do
    echo "------------------------ $num_thread thread  8 wh--------------------------"
    echo "------------------------ $num_thread thread  8 wh--------------------------" >> $name
    for ((i=0;i<$1;i++));
    do
        ./out-perf.masstree/benchmarks/dbtest --bench tpcc --parallel-loading --retry-aborted-transactions --bench-opt "--workload-mix 45,43,4,4,4" --db-type ndb-proto2  --backoff-aborted-transactions --runtime 30 --num-threads $num_thread --scale-factor 8 >> $name
    done
done

echo "Finish Test"