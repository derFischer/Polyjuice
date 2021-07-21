#!/bin/bash
runtime=30
count=5

echo "------ Figure 8(a) Evaluation Start ------"

cd ae-tpce-polyjuice
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-e figure8(a) Polyjuice"
for theta in {0.0,1.0,2.0,3.0,4.0}
do
    for((i=1;i<=$count;i++));
    do
        threads=48
        echo "tpc-e. cc=Polyjuice. num-threads=$threads, zipf_theta=$theta"
        policy="../ae-policy/tpce/${threads}th-${theta}theta.txt"
        ./out-perf.masstree/benchmarks/dbtest \
            --bench tpce \
            --parallel-loading \
            --retry-aborted-transactions \
            --bench-opt "-w 0,0,0,0,0,0,50,0,0,50 -m 0 -s 1 -a $theta" \
            --db-type ndb-ic3 \
            --backoff-aborted-transactions \
            --runtime $runtime \
            --num-threads $threads \
            --scale-factor 1 \
            --policy $policy
    done
done
cd ..

cd ae-tpce-silo
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-e figure8(a) Silo"
for theta in {0.0,1.0,2.0,3.0,4.0}
do
    for((i=1;i<=$count;i++));
    do
        threads=48
        echo "tpc-e. cc=Silo. num-threads=$threads, zipf_theta=$theta"
        ./out-perf.masstree/benchmarks/dbtest \
            --bench tpce \
            --parallel-loading \
            --retry-aborted-transactions \
            --bench-opt "-w 0,0,0,0,0,0,50,0,0,50 -m 0 -s 1 -a $theta" \
            --db-type ndb-proto2 \
            --backoff-aborted-transactions \
            --runtime $runtime \
            --num-threads $threads \
            --scale-factor 1
    done
done
cd ..

cd ae-tpce-2pl
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-e figure8(a) 2PL"
for theta in {0.0,1.0,2.0,3.0,4.0}
do
    for((i=1;i<=$count;i++));
    do
        threads=48
        echo "tpc-e. cc=2PL. num-threads=$threads, zipf_theta=$theta"
        ./out-perf.masstree/benchmarks/dbtest \
            --bench tpce \
            --parallel-loading \
            --retry-aborted-transactions \
            --bench-opt "-w 0,0,0,0,0,0,50,0,0,50 -m 0 -s 1 -a $theta" \
            --db-type ndb-proto2 \
            --backoff-aborted-transactions \
            --runtime $runtime \
            --num-threads $threads \
            --scale-factor 1
    done
done
cd ..

echo "------ Figure 8(a) Evaluation Finish ------"
