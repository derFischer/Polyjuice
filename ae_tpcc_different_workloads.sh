#!/bin/bash
runtime=30
count=5

echo "------ Figure 12 Evaluation Start ------"

cd ae-tpcc-polyjuice
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c figure12(a) Polyjuice"
for warehouse in {1,2,4,8,12,16,48}
do
    for((i=1;i<=$count;i++));
    do
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
    done
done

echo "tpc-c figure12(a) Polyjuice-1 warehouse policy"
for warehouse in {1,2,4,8,12,16,48}
do
    for((i=1;i<=$count;i++));
    do
        threads=48
        echo "tpc-c. cc=Polyjuice-1 warehouse policy. num-threads=$threads, warehouse-num=$warehouse"
        policy="../ae-policy/tpcc/${threads}th-1wh.txt"
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
done

echo "tpc-c figure12(a) Polyjuice-4 warehouse policy"
for warehouse in {1,2,4,8,12,16,48}
do
    for((i=1;i<=$count;i++));
    do
        threads=48
        echo "tpc-c. cc=Polyjuice-4 warehouse policy. num-threads=$threads, warehouse-num=$warehouse"
        policy="../ae-policy/tpcc/${threads}th-4wh.txt"
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
done

cd ..

cd ae-tpcc-silo
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c figure12(a) Silo"
for warehouse in {1,2,4,8,12,16,48}
do
    for((i=1;i<=$count;i++));
    do
        threads=48
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
done
cd ..

cd ae-tpcc-2pl
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c figure12(a) 2PL"
for warehouse in {1,2,4,8,12,16,48}
do
    for((i=1;i<=$count;i++));
    do
        threads=48
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
done
cd ..

echo "tpc-c figure12(a) cormcc"
echo "CormCC performance pick max(2PL,Silo) as its result, refered to Section 7.2 in the paper."



cd ae-tpcc-polyjuice
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c figure12(b) Polyjuice"
for threads in {1,2,4,8,12,16,32,48}
do
    for((i=1;i<=$count;i++));
    do
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
done

echo "tpc-c figure12(b) Polyjuice-48 thread policy"
for threads in {1,2,4,8,12,16,32,48}
do
    for((i=1;i<=$count;i++));
    do
        warehouse=1
        echo "tpc-c. cc=Polyjuice-1 warehouse policy. num-threads=$threads, warehouse-num=$warehouse"
        policy="../ae-policy/tpcc/48th-1wh.txt"
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
done

echo "tpc-c figure12(b) Polyjuice-16 thread policy"
for threads in {1,2,4,8,12,16,32,48}
do
    for((i=1;i<=$count;i++));
    do
        warehouse=1
        echo "tpc-c. cc=Polyjuice-4 warehouse policy. num-threads=$threads, warehouse-num=$warehouse"
        policy="../ae-policy/tpcc/16th-1wh.txt"
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
done

cd ..

cd ae-tpcc-silo
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c figure12(b) Silo"
for threads in {1,2,4,8,12,16,32,48}
do
    for((i=1;i<=$count;i++));
    do
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
done
cd ..

cd ae-tpcc-2pl
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c figure12(b) 2PL"
for threads in {1,2,4,8,12,16,32,48}
do
    for((i=1;i<=$count;i++));
    do
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
done
cd ..

echo "tpc-c figure12(b) cormcc"
echo "CormCC performance pick max(2PL,Silo) as its result, refered to Section 7.2 in the paper."

echo "------ Figure 4(a,b) Evaluation Finish ------"
