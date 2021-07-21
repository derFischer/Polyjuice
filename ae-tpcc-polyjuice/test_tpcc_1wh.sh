#!/bin/bash

usage() {
    echo "usage: ./test.sh <N> <nw>"
    exit -1
}

name=/home/jiachen/artifact_eval/ae-tpcc-poly/evaluation/test_result_tpcc_1wh

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


policypath=th-1wh.txt


for num_thread in 1 2 4 8 12 16 24 32 48
do
    echo "------------------------ $num_thread thread  1 wh --------------------------"
    echo "------------------------ $num_thread thread  1 wh  --------------------------" >> $name
    
    for ((i=0;i<$1;i++));
    do
        ./out-perf.masstree/benchmarks/dbtest --bench tpcc --parallel-loading --retry-aborted-transactions --bench-opt "--workload-mix 45,43,4,4,4" --db-type ndb-ic3 --backoff-aborted-transactions --runtime 30 --num-threads $num_thread --scale-factor 1 --policy /home/jiachen/artifact_eval/ae-policy/tpcc/$num_thread$policypath >> $name
    done
done



echo "Finish Test"