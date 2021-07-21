#!/bin/bash

usage() {
    echo "usage: ./test.sh <N> <nw>"
    exit -1
}

name=/home/jiachen/artifact_eval/ae-tpcc-poly/evaluation/test_result_tpcc_48wh

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

policypath_f=48th-
policypath_l=wh.txt


for num_wh in 1 2 4 8 12 16 24 48
do
    echo "------------------------ 48 thread  $num_wh wh --------------------------"
    echo "------------------------ 48 thread  $num_wh wh  --------------------------" >> $name
    
    for ((i=0;i<$1;i++));
    do
        ./out-perf.masstree/benchmarks/dbtest --bench tpcc --parallel-loading --retry-aborted-transactions --bench-opt "--workload-mix 45,43,4,4,4" --db-type ndb-ic3 --backoff-aborted-transactions --runtime 30 --num-threads 48 --scale-factor $num_wh --policy /home/jiachen/artifact_eval/ae-policy/tpcc/$policypath_f$num_wh$policypath_l >> $name
    done
done


echo "Finish Test"