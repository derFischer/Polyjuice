#!/bin/bash
runtime=30
count=5

echo "------ Figure 5 Evaluation Start ------"

cd ae-tpcc-polyjuice
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c figure5 EA-Polyjuice"
for((i=1;i<=$count;i++));
do
    threads=48
    warehouse=1
    echo "tpc-c. training_method=EA-Polyjuice. num-threads=$threads, warehouse-num=$warehouse"
    python training/ERL_main.py \
        --graph \
        --workload-type tpcc \
        --bench-opt "-w 45,43,4,4,4" \
        --scale-factor $warehouse \
        --max-iteration 300 \
        --eval-time 1.0 \
        --psize 8 \
        --random-branch 4 \
        --mutate-rate 0.05 \
        --nworkers $threads \
        --pickup-policy training/input-RL-ic3-tpcc.txt \
        --expr-name "EA-Polyjuice-"
done
cd ..

cd ae-tpcc-polyjuice-rl
# make dbtest -j
# echo "------ Make Done ------"
echo "tpc-c figure5 RL"
for((i=1;i<=$count;i++));
do
    threads=48
    warehouse=1
    echo "tpc-c. training_method=RL. num-threads=$threads, warehouse-num=$warehouse"
    python training/ERL_main.py \
    --graph \
    --workload-type tpcc \
    --scale-factor $warehouse \
    --max-iteration 300 \
    --eval-time 1.0 \
    --sync-period 2 \
    --samples-per-distribution 32 \
    --learning-rate 0.00002 \
    --reward-decay 0.9 \
    --psize 8 \
    --random-branch 0 \
    --mutate-rate 0.05 \
    --nworkers $threads \
    --pickup-policy training/input-RL-ic3-tpcc.txt \
    --expr-name "RL-"
done
cd ..

echo "------ Figure 5 Evaluation Finish ------"
