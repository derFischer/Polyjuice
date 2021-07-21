#!/bin/bash

usage() {
    echo "usage: ./test.sh <N> <nw>"
    exit -1
}

name=test_result_micro_occ_scalability.txt

if [ "$#" -ne 2 ]; then
    usage
elif ! [[ $1 =~ ^[0-9]+$ ]] ; then
    echo "$1 is not a number"
    usage
elif ! [[ $2 =~ ^[0-9]+$ ]] ; then
    echo "$2 is not a number"
    usage
fi

echo "TEST const key range 8, 32" > $name


for thread in 1 2 4 8 16 24 48 
do
    echo "------------------------ $thread thread  8 range--------------------------"
    echo "------------------------ $thread thread  8 range--------------------------" >> $name
    for ((i=0;i<$1;i++));
    do
        ./out-perf.masstree/benchmarks/dbtest --bench microbench --retry-aborted-transactions --bench-opt "-a 8 -p 4" --db-type ndb-proto2  --backoff-aborted-transactions --runtime 30 --num-threads $thread  >> $name
    done
done


for thread in 1 2 4 8 16 24 48 
do
    echo "------------------------ $thread thread  32 range--------------------------"
    echo "------------------------ $thread thread  32 range--------------------------" >> $name
    for ((i=0;i<$1;i++));
    do
        ./out-perf.masstree/benchmarks/dbtest --bench microbench --retry-aborted-transactions --bench-opt "-a 32 -p 4" --db-type ndb-proto2  --backoff-aborted-transactions --runtime 30 --num-threads $thread  >> $name
    done
done

echo "Finish Test"