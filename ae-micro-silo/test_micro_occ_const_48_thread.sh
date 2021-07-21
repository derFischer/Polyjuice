#!/bin/bash

usage() {
    echo "usage: ./test.sh <N> <nw>"
    exit -1
}

name=test_result_micro_occ_const_48_thread.txt

if [ "$#" -ne 2 ]; then
    usage
elif ! [[ $1 =~ ^[0-9]+$ ]] ; then
    echo "$1 is not a number"
    usage
elif ! [[ $2 =~ ^[0-9]+$ ]] ; then
    echo "$2 is not a number"
    usage
fi

echo "TEST const 48 thread" > $name


for range in 4 8 16 32 64 128 
do
    echo "------------------------ 48 thread  $range range--------------------------"
    echo "------------------------ 48 thread  $range range--------------------------" >> $name
    for ((i=0;i<$1;i++));
    do
        ./out-perf.masstree/benchmarks/dbtest --bench microbench --retry-aborted-transactions --bench-opt "-a $range -p 4" --db-type ndb-proto2  --backoff-aborted-transactions --runtime 30 --num-threads 48  >> $name
    done
done

echo "Finish Test"