#!/bin/bash

usage() {
    echo "usage: ./test.sh <N> <nw>"
    exit -1
}

name=z_test_result_tpcc

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



# echo "------------------------ 4 thread  1 wh --------------------------"
# echo "------------------------ 4 thread  1 wh  --------------------------" >> $name
# for ((i=0;i<$1;i++));
#     do
#         ./out-perf.masstree/benchmarks/dbtest --bench tpcc --parallel-loading --retry-aborted-transactions --bench-opt "--workload-mix 45,43,4,4,4" --db-type ndb-ic3 --backoff-aborted-transactions --runtime 30 --num-threads 4 --scale-factor 1 --policy /home/jiachen/artifact_eval/OSDI21-submit-policy/tpcc-policy/4th-1wh/genetic_best.txt  >> $name
# done

# echo "------------------------ 8 thread  1 wh --------------------------"
# echo "------------------------ 8 thread  1 wh  --------------------------" >> $name
# for ((i=0;i<$1;i++));
#     do
#         ./out-perf.masstree/benchmarks/dbtest --bench tpcc --parallel-loading --retry-aborted-transactions --bench-opt "--workload-mix 45,43,4,4,4" --db-type ndb-ic3 --backoff-aborted-transactions --runtime 30 --num-threads 8 --scale-factor 1 --policy /home/jiachen/artifact_eval/OSDI21-submit-policy/tpcc-policy/8th-1wh/genetic_best.txt  >> $name
# done




for num_wh in {1 4 8 12 16 24 32 48} 
do
    echo "------------------------ $num_wh thread  1 wh --------------------------"
    echo "------------------------ $num_wh thread  1 wh  --------------------------" >> $name
    
    for ((i=0;i<$1;i++));
    do
        ./out-perf.masstree/benchmarks/dbtest --bench tpcc --parallel-loading --retry-aborted-transactions --bench-opt "--workload-mix 45,43,4,4,4" --db-type ndb-ic3 --backoff-aborted-transactions --runtime 30 --num-threads $num_wh --scale-factor 1 --policy /home/jiachen/artifact_eval/OSDI21-submit-policy/tpcc-policy/{$num_wh}th-1wh/genetic_best.txt  >> $name
    done
done



echo "Finish Test"