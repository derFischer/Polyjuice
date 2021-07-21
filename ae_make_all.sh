#!/bin/bash

echo "------ Make for all repos -> Start ------"

echo "------ TPC-C Related ------"

cd ae-tpcc-polyjuice
make clean
make dbtest -j
cd ..

cd ae-tpcc-silo
make clean
make dbtest -j
cd ..

cd ae-tpcc-2pl
make clean
make dbtest -j
cd ..

echo "------ Latency Related ------"

cd ae-tpcc-polyjuice-latency
make clean
make dbtest -j
cd ..

cd ae-tpcc-silo-latency
make clean
make dbtest -j
cd ..

cd ae-tpcc-2pl-latency
make clean
make dbtest -j
cd ..

echo "------ Factor Analysis Related ------"

cd ae-tpcc-factor-no-dirty-read-public-write
make clean
make dbtest -j
cd ..

cd ae-tpcc-factor-no-coarse-grained-waiting
make clean
make dbtest -j
cd ..

cd ae-tpcc-factor-no-fine-grained-waiting
make clean
make dbtest -j
cd ..

echo "------ TPC-E Related ------"

cd ae-tpce-polyjuice
make clean
make dbtest -j
cd ..

cd ae-tpce-silo
make clean
make dbtest -j
cd ..

cd ae-tpce-2pl
make clean
make dbtest -j
cd ..

echo "------ Microbenchmark Related ------"

cd ae-micro-polyjuice
make clean
make dbtest -j
cd ..

cd ae-micro-silo
make clean
make dbtest -j
cd ..

cd ae-micro-2pl
make clean
make dbtest -j
cd ..

echo "------ Training Related ------"

cd ae-tpcc-polyjuice-rl
make clean
make dbtest -j
cd ..

echo "------ Make for all repos -> Finish ------"
