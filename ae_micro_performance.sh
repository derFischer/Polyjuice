#!/bin/bash
runtime=30
count=5

echo "------ Figure 9 Evaluation Start ------"

cd ae-micro-polyjuice
# make dbtest -j
# echo "------ Make Done ------"
echo "micro-benchmark(10 tx type) figure9 Polyjuice"
for theta in {0.2,0.4,0.6,0.8,1.0}
do
    for((i=1;i<=$count;i++));
    do
        threads=48
        echo "micro-benchmark(10 tx type). cc=Polyjuice. num-threads=$threads, zipf_theta=$theta"
        policy="../ae-policy/micro/${threads}th-${theta}theta.txt"
        ./out-perf.masstree/benchmarks/dbtest \
            --bench microbench \
            --retry-aborted-transactions \
            --bench-opt "-a 4096 -p 6 -z $theta" \
            --db-type ndb-ic3 \
            --backoff-aborted-transactions \
            --runtime $runtime \
            --num-threads $threads \
            --policy $policy
    done
done
cd ..

cd ae-micro-silo
# make dbtest -j
# echo "------ Make Done ------"
echo "micro-benchmark(10 tx type) figure9 Silo"
for theta in {0.2,0.4,0.6,0.8,1.0}
do
    for((i=1;i<=$count;i++));
    do
        threads=48
        echo "micro-benchmark(10 tx type). cc=Silo. num-threads=$threads, zipf_theta=$theta"
        ./out-perf.masstree/benchmarks/dbtest \
            --bench microbench \
            --retry-aborted-transactions \
            --bench-opt "-a 4096 -p 6 -z $theta" \
            --db-type ndb-proto2 \
            --backoff-aborted-transactions \
            --runtime $runtime \
            --num-threads $threads
    done
done
cd ..

cd ae-micro-2pl
# make dbtest -j
# echo "------ Make Done ------"
echo "micro-benchmark(10 tx type) figure9 2PL"
for theta in {0.2,0.4,0.6,0.8,1.0}
do
    for((i=1;i<=$count;i++));
    do
        threads=48
        echo "micro-benchmark(10 tx type). cc=2PL. num-threads=$threads, zipf_theta=$theta"
        ./out-perf.masstree/benchmarks/dbtest \
            --bench microbench \
            --retry-aborted-transactions \
            --bench-opt "-a 4096 -p 6 -z $theta" \
            --db-type ndb-proto2 \
            --backoff-aborted-transactions \
            --runtime $runtime \
            --num-threads $threads
    done
done
cd ..

echo "------ Figure 9 Evaluation Finish ------"
