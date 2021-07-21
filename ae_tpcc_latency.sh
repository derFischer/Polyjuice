#!/bin/bash
runtime=30
count=5

echo "------ Table 2 Evaluation Start ------"

cd ae-tpcc-polyjuice-latency
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c Table 2 Polyjuice"
for((i=1;i<=$count;i++));
do
    threads=48
    warehouse=1
    echo "tpc-c. cc=Polyjuice. num-threads=$threads, warehouse-num=$warehouse"
    policy="../ae-policy/tpcc/${threads}th-${warehouse}wh.txt"
    ./out-perf.masstree/benchmarks/dbtest \
        --bench tpcc \
        --parallel-loading \
        --retry-aborted-transactions \
        --bench-opt "--workload-mix 45,43,4,4,4" \
        --db-type ndb-ic3 \
        --backoff-aborted-transactions \
        --runtime $runtime \
        --num-threads $threads \
        --scale-factor $warehouse \
        --policy $policy
done
cd ..

cd ae-tpcc-silo-latency
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c Table 2 Silo"
for((i=1;i<=$count;i++));
do
    threads=48
    warehouse=1
    echo "tpc-c. cc=Silo. num-threads=$threads, warehouse-num=$warehouse"
    ./out-perf.masstree/benchmarks/dbtest \
        --bench tpcc \
        --parallel-loading \
        --retry-aborted-transactions \
        --bench-opt "--workload-mix 45,43,4,4,4" \
        --db-type ndb-proto2 \
        --backoff-aborted-transactions \
        --runtime $runtime \
        --num-threads $threads \
        --scale-factor $warehouse
done
cd ..

cd ae-tpcc-2pl-latency
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c Table 2 2PL"
for((i=1;i<=$count;i++));
do
    threads=48
    warehouse=1
    echo "tpc-c. cc=2PL. num-threads=$threads, warehouse-num=$warehouse"
    ./out-perf.masstree/benchmarks/dbtest \
        --bench tpcc \
        --parallel-loading \
        --retry-aborted-transactions \
        --bench-opt "--workload-mix 45,43,4,4,4" \
        --db-type ndb-proto2 \
        --backoff-aborted-transactions \
        --runtime $runtime \
        --num-threads $threads \
        --scale-factor $warehouse
done
cd ..

echo "tpc-c Table 2 cormcc"
echo "CormCC performance pick max(2PL,Silo) as its result, so CormCC latency goes to  refered to Section 7.2 in the paper."

echo "------ Table 2 Evaluation Finish ------"
