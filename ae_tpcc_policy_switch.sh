#!/bin/bash
runtime=30
count=5

echo "------ Figure 10 Evaluation Start ------"

cd ae-tpcc-polyjuice-policy-switch
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c figure10 Polyjuice"
warehouse=1
threads=48
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
cd ..

echo "------ Figure 10 Evaluation Finish ------"
