#!/bin/bash

usage() {
    echo "usage: ./test.sh <N> <nw>"
    exit -1
}

name=test_result_tpcc_occ_const_48_thread.txt

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


for num_wh in 1 2 4 8 12 16 24 48 
do
    echo "------------------------ 48 thread  $num_wh wh--------------------------"
    echo "------------------------ 48 thread  $num_wh wh--------------------------" >> $name
    for ((i=0;i<$1;i++));
    do
        # ./out-perf.masstree/benchmarks/dbtest --bench tpcc --parallel-loading --retry-aborted-transactions --bench-opt "--workload-mix 45,43,4,4,4" --db-type ndb-ic3  --backoff-aborted-transactions --runtime 30 --num-threads 16 --scale-factor 1 --policy policy/16th-1wh/16th-1wh-1/genetic_best.txt  >> $name
        # ./out-perf.masstree/benchmarks/dbtest --bench tpce --parallel-loading --retry-aborted-transactions --db-type ndb-ic3  --backoff-aborted-transactions --scale-factor 1 --bench-opt "-w 0,0,0,0,0,0,50,0,0,50 -m 0 -s 8" --runtime $2 --num-threads 24 --policy training/input-RL-ic3-tpce.txt  >> $name
        ./out-perf.masstree/benchmarks/dbtest --bench tpcc --parallel-loading --retry-aborted-transactions --bench-opt "--workload-mix 45,43,4,4,4" --db-type ndb-proto2  --backoff-aborted-transactions --runtime 30 --num-threads 48 --scale-factor $num_wh >> $name
    done
done

echo "Finish Test"