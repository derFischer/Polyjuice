#!/bin/bash
runtime=30
count=5
threads=48
high_contention_wh=1
moderate_contention_wh=8

echo "------ Figure 6(a,b) Evaluation Start ------"

cd ae-tpcc-factor-no-dirty-read-public-write
# make dbtest -j
# echo "------ Make Done ------"

echo "tpc-c figure6(a) high contention [occ policy]"
for((i=1;i<=$count;i++));
do
    warehouse=$high_contention_wh
    echo "tpc-c. setting=[occ policy]. num-threads=$threads, warehouse-num=$warehouse"
    policy="../ae-policy/factor-analysis/occ-policy.txt"
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
echo "tpc-c figure6(b) moderate contention [occ policy]"
for((i=1;i<=$count;i++));
do
    warehouse=$moderate_contention_wh
    echo "tpc-c. setting=[occ policy]. num-threads=$threads, warehouse-num=$warehouse"
    policy="../ae-policy/factor-analysis/occ-policy.txt"
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

echo "tpc-c figure6(a) high contention [+ early validation]"
for((i=1;i<=$count;i++));
do
    warehouse=$high_contention_wh
    echo "tpc-c. setting=[+ early validation]. num-threads=$threads, warehouse-num=$warehouse"
    policy="../ae-policy/factor-analysis/${threads}th-${warehouse}wh-bar2.txt"
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
echo "tpc-c figure6(b) moderate contention [+ early validation]"
for((i=1;i<=$count;i++));
do
    warehouse=$moderate_contention_wh
    echo "tpc-c. setting=[+ early validation]. num-threads=$threads, warehouse-num=$warehouse"
    policy="../ae-policy/factor-analysis/${threads}th-${warehouse}wh-bar2.txt"
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

cd ae-tpcc-factor-no-coarse-grained-waiting
# make dbtest -j
# echo "------ Make Done ------"

echo "tpc-c figure6(a) high contention [+ dirty read & public write]"
for((i=1;i<=$count;i++));
do
    warehouse=$high_contention_wh
    echo "tpc-c. setting=[+ dirty read & public write]. num-threads=$threads, warehouse-num=$warehouse"
    policy="../ae-policy/factor-analysis/${threads}th-${warehouse}wh-bar3.txt"
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
echo "tpc-c figure6(b) moderate contention [+ dirty read & public write]"
for((i=1;i<=$count;i++));
do
    warehouse=$moderate_contention_wh
    echo "tpc-c. setting=[+ dirty read & public write]. num-threads=$threads, warehouse-num=$warehouse"
    policy="../ae-policy/factor-analysis/${threads}th-${warehouse}wh-bar3.txt"
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

cd ae-tpcc-factor-no-fine-grained-waiting
# make dbtest -j
# echo "------ Make Done ------"

echo "tpc-c figure6(a) high contention [+ coarse grained waiting]"
for((i=1;i<=$count;i++));
do
    warehouse=$high_contention_wh
    echo "tpc-c. setting=[+ coarse grained waiting]. num-threads=$threads, warehouse-num=$warehouse"
    policy="../ae-policy/factor-analysis/${threads}th-${warehouse}wh-bar4.txt"
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
echo "tpc-c figure6(b) moderate contention [+ coarse grained waiting]"
for((i=1;i<=$count;i++));
do
    warehouse=$moderate_contention_wh
    echo "tpc-c. setting=[+ coarse grained waiting]. num-threads=$threads, warehouse-num=$warehouse"
    policy="../ae-policy/factor-analysis/${threads}th-${warehouse}wh-bar4.txt"
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

cd ae-tpcc-polyjuice
# make dbtest -j
# echo "------ Make Done ------"

echo "tpc-c figure6(a) high contention [+ fine grained waiting]"
for((i=1;i<=$count;i++));
do
    warehouse=$high_contention_wh
    echo "tpc-c. setting=[+ fine grained waiting]. num-threads=$threads, warehouse-num=$warehouse"
    policy="../ae-policy/factor-analysis/${threads}th-${warehouse}wh-bar5.txt"
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
echo "tpc-c figure6(b) moderate contention [+ fine grained waiting]"
for((i=1;i<=$count;i++));
do
    warehouse=$moderate_contention_wh
    echo "tpc-c. setting=[+ fine grained waiting]. num-threads=$threads, warehouse-num=$warehouse"
    policy="../ae-policy/factor-analysis/${threads}th-${warehouse}wh-bar5.txt"
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

echo "------ Figure 6(a,b) Evaluation Finish ------"
