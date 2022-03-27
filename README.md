# Polyjuice Artifact Evaluation
## Part 1. Getting Started

**Warning: As is said before, the training and learned policy is based on specific hardwares. If you decide to evaluate on your own machine, it may not have the same performance as ours.**

#### 1.1 Download 
The source code is at GitLab. You can download the source code with the following command:
```
user@host[~]$ git clone https://github.com/derFischer/Polyjuice.git
```
For each CC and benchmark, we set an individual folder so you can easily evaluate on different settings. Part 2.1 shows the detailed description for each folder.

#### 1.2 Build
**On our machine, we use Python 3.6.9 and GCC 7.5.0/8.3.0. We recommend you to use the same versions of Python and GCC as ours because otherwise it may fail to compile the codes.**

##### 1.2.1 Library Dependency
Our project depends on libraries as listed and we give the install command on Ubuntu 18.04 with apt-get.

| Library | Install Command |
| ---- | --- |
| libnuma |  apt-get install libnuma-dev
| libdb |  apt-get install libdb-dev libdb++-dev
| libaio |  apt-get install libaio-dev
| libz |  apt-get install libz-dev
| libssl |  apt-get install libssl-dev
| autoconf |  apt-get install autoconf
| jemalloc | apt-get install libjemalloc-dev

 </br>

##### 1.2.2 Training Support
Training code is meant to run in a Python3 environment. Below we describe the setup
using a [Python Virtual Environment](https://docs.python.org/3/tutorial/venv.html). This
can either be done with the `virtualenv` package, or the Python3 `venv` package.

First set up a virtual environment.
```
user@host[~]$ python3 -m venv sim_env
```
Next activate the environment and enter the repository directory.
```
user@host[~]$ source sim_env/bin/activate
(sim_env)user@host[~]$ cd Polyjuice
```
Install needed libraries.
```
(sim_env)user@host[~/Polyjuice]$ pip install --upgrade pip
(sim_env)user@host[~/Polyjuice]$ pip install --upgrade numpy
(sim_env)user@host[~/Polyjuice]$ pip install tensorflow==1.14.0     # we use this version specifically
(sim_env)user@host[~/Polyjuice]$ pip install --upgrade tensorboard_logger
```

##### 1.2.3 Build binaries
You can try the following commands to test whether all the dependencies have been installed and can successfully run the code. **In our evaluation, we need to build the binaries in each folder. However, we also provide a script that automatically builds all the binaries in all folders.**

```
user@host[~]$ cd Polyjuice
user@host[~/Polyjuice]$ cd ae-tpcc-polyjuice
user@host[~/Polyjuice/ae-tpcc-polyjuice]$ make dbtest -j
```
If it is built successfully, you should be able to see the following output:
```
g++ -o out-perf.masstree/benchmarks/dbtest out-perf.masstree/benchmarks/dbtest.o out-perf.masstree/allocator.o out-perf.masstree/btree.o out-perf.masstree/core.o out-perf.masstree/counter.o out-perf.masstree/memory.o out-perf.masstree/rcu.o out-perf.masstree/stats_server.o out-perf.masstree/thread.o out-perf.masstree/ticker.o out-perf.masstree/tuple.o out-perf.masstree/txn_btree.o out-perf.masstree/txn.o out-perf.masstree/txn_ic3_impl.o out-perf.masstree/varint.o out-perf.masstree/txn_entry_impl.o out-perf.masstree/policy.o out-perf.masstree/compiler.o out-perf.masstree/str.o out-perf.masstree/string.o out-perf.masstree/straccum.o out-perf.masstree/json.o out-perf.masstree/benchmarks/ldb_wrapper.o out-perf.masstree/benchmarks/bdb_wrapper.o out-perf.masstree/benchmarks/bench.o out-perf.masstree/benchmarks/encstress.o out-perf.masstree/benchmarks/masstree/kvrandom.o out-perf.masstree/benchmarks/queue.o out-perf.masstree/benchmarks/tpcc.o out-perf.masstree/benchmarks/tpce.o out-perf.masstree/benchmarks/micro_badcount.o out-perf.masstree/benchmarks/micro_lock_perf.o out-perf.masstree/benchmarks/micro_ic3_perf.o out-perf.masstree/benchmarks/micro_range.o out-perf.masstree/benchmarks/micro_delete.o out-perf.masstree/benchmarks/micro_insert.o out-perf.masstree/benchmarks/micro_transitive.o out-perf.masstree/benchmarks/micro_transitive2.o out-perf.masstree/benchmarks/micro_mem.o out-perf.masstree/benchmarks/micro_bench.o out-perf.masstree/benchmarks/micro_lock.o out-perf.masstree/benchmarks/ycsb.o out-perf.masstree/benchmarks/smallbank.o third-party/lz4/liblz4.so egen/egenlib/egenlib.a -lpthread -lnuma -lrt -ljemalloc -ldb_cxx  -Lthird-party/lz4 -llz4 -Wl,-rpath,/home/jiachen/Polyjuice/ae-tpcc-polyjuice/third-party/lz4
```

Then, try the following command to run Polyjuice on TPC-C benchmark, 1 warehouse with 1 thread.

```
user@host[~/Polyjuice/ae-tpcc-polyjuice]$ ./out-perf.masstree/benchmarks/dbtest --bench tpcc --parallel-loading --retry-aborted-transactions --bench-opt "--workload-mix 45,43,4,4,4" --db-type ndb-ic3 --backoff-aborted-transactions --runtime 30 --num-threads 1 --scale-factor 1 --policy training/input-RL-occ-tpcc.txt
```

After about 30 seconds, you should be able to see the following output:
```
RESULT throughput(77511.3),agg_abort_rate(0)
```

**If you can successfully run the commands above, then you can run the following commands to build all the binaries**:
```
user@host[~/Polyjuice]$ ./ae_make_all.sh
```

## Part 2. Step-by-Step instructions
We mainly provide the artifact to confirm our performance results, namely Figure 4-6, Figure 8-10, Figure 12 and Table 2 of the evaluation section.

### 2.1 Important notes
1. Most of our experiments will use 48 **physical** cores. Using fewer cores or hyper-threads might produce different performance results.
2. To quickly reproduce the result in the paper, for Figure 4, 6, 8, 9, 10, 12 and Table 2, we provide the policies learned in our evaluation.

### 2.2 Directory structure
The following shows the detailed description for each folder in `./Polyjuice`. However, you **don't** have to remember what is each folder used for. Our scripts in Part 2.3 will automatically `cd` into corresponding folders before running the experiments.

**TPC-C Benchmark**

**Figure 4: TPC-C Performance and Scalability** &

**Figure 12: Throughput under different workloads, 48 threads**

| folder | description |
| ---- | --- |
| ae-tpcc-polyjuice | Polyjuice performance on TPC-C benchmark

 </br>

**Figure 10: Throughput during policy switch**

| folder | description |
| ---- | --- |
| ae-tpcc-polyjuice-policy-switch | Polyjuice performance during policy switch

 </br>

**Figure 6: Factor Analysis On TPC-C Benchmark**

| folder | description |
| ---- | --- |
| ae-tpcc-polyjuice | Factor analysis, bar 1 & bar 5
| ae-tpcc-factor-no-dirty-read-public-write |  Factor analysis, bar 2
| ae-tpcc-factor-no-coarse-grained-waiting |  Factor analysis, bar 3
| ae-tpcc-factor-no-fine-grained-waiting |  Factor analysis, bar 4

 </br>

**Table 2: Latency for each transaction type in TPC-C with 1 warehouse and 48 threads**

| folder | description |
| ---- | --- |
| ae-tpcc-polyjuice-latency | Polyjuice latency on TPC-C benchmark

 </br>

------

**TPC-E Benchmark**

**Figure 8: TPC-E Performance and Scalability**

| folder | description |
| ---- | --- |
| ae-tpce-polyjuice | Polyjuice performance on TPC-E benchmark

 </br>

------

**Micro Benchmark**

**Figure 9: Micro-benchmark (10 tx types)**

| folder | description |
| ---- | --- |
| ae-micro-polyjuice | Polyjuice performance on TPC-E benchmark

 </br>

------

**Training**

**Figure 5: Training Efficiency**

| folder | description |
| ---- | --- |
| ae-tpcc-polyjuice | Polyjuice training on TPC-C benchmark
| ae-tpcc-polyjuice-rl |  RL training on TPC-C benchmark

 </br>

### 2.3 Run Experiments
All the policies used in the evaluation are saved in the directory `./Polyjuice/ae-policy/`.

Note that all the scripts in the following part are saved in the directory `./Polyjuice`, and **all of them do not need to specify any parameter**. For example, to get the scalability of TPC-C (Figure 4.(c)), you can simply run:

```
user@host[~/Polyjuice]$ ./ae_tpcc_scalability.sh
```

| script | Experiment | Corresponding Figure
| ---- | --- | --- |
| ae_tpcc_performance.sh | TPC-C performance | Figure 4.(a) 4.(b)
| ae_tpcc_scalability.sh |  TPC-C scalability | Figure 4.(c)
| ae_tpcc_factor-analysis.sh |  Polyjuice factor-analysis performance | Figure 6
| ae_tpcc_latency.sh |  TPC-C latency | Table 2
| ae_tpcc_policy_switch.sh | Polyjuice performance during policy switch | Figure 10
| ae_tpce_performance.sh | TPC-E performance | Figure 8.(a)
| ae_tpce_scalability.sh | TPC-E scalability | Figure 8.(b)
| ae_micro_performance.sh | Micro performance | Figure 9
| ae_tpcc_different_workloads.sh | TPC-C different workloads performance | Figure 12

 </br>

It may take 20 minutes+ to take each experiment above.

For Figure 5, we provide the script for training. Since the training result may not be stable, for each setting, the script will train for 5 times. Each training cost about 6 hours.

**Note that the following commands need tensorflow library and please ensure that you have activated the virtual environment (see Part 1.2.2) before executing these commands.**

| script | Experiment | Corresponding Figure
| ---- | --- | --- |
| ae_ea_rl.sh |  EA v.s. RL efficiency | Figure 5

Retraining Figure 5 may take 2\*5\*6 = 60 hours.


### 2.4 Output format
#### 2.4.1 Performance output
The scripts will automatically run all the settings in the corresponding figure, and each setting for **5 times**. To get a stable result, each run costs 30 seconds.

**TPC-C performance**
After executing `./ae_tpcc_performance.sh`, you should be able to see:
```
------ Figure 4(a,b) Evaluation Start ------
make: Nothing to be done for 'dbtest'.
------ Make Done ------
tpc-c figure4(a,b) Polyjuice
tpc-c. cc=Polyjuice. num-threads=48, warehouse-num=1
RESULT throughput(304165),agg_abort_rate(0.0291986)
tpc-c. cc=Polyjuice. num-threads=48, warehouse-num=1
RESULT throughput(302750),agg_abort_rate(0.0285134)
tpc-c. cc=Polyjuice. num-threads=48, warehouse-num=1
RESULT throughput(291854),agg_abort_rate(0.0292322)
tpc-c. cc=Polyjuice. num-threads=48, warehouse-num=1
RESULT throughput(303681),agg_abort_rate(0.0294943)
tpc-c. cc=Polyjuice. num-threads=48, warehouse-num=1
RESULT throughput(294665),agg_abort_rate(0.0287176)
...
```

**TPC-C scalability**
After executing `./ae_tpcc_scalability.sh`, you should be able to see:
```
------ Figure 4(c) Evaluation Start ------
make: Nothing to be done for 'dbtest'.
------ Make Done ------
tpc-c figure4(c) Polyjuice
tpc-c. cc=Polyjuice. num-threads=1, warehouse-num=1
RESULT throughput(77448.4),agg_abort_rate(0)
tpc-c. cc=Polyjuice. num-threads=1, warehouse-num=1
RESULT throughput(77818.1),agg_abort_rate(0)
tpc-c. cc=Polyjuice. num-threads=1, warehouse-num=1
RESULT throughput(78000.4),agg_abort_rate(0)
tpc-c. cc=Polyjuice. num-threads=1, warehouse-num=1
RESULT throughput(78087.8),agg_abort_rate(0)
tpc-c. cc=Polyjuice. num-threads=1, warehouse-num=1
RESULT throughput(77787.8),agg_abort_rate(0)
...
```

**Polyjuice factor-analysis performance**
After executing `./ae_tpcc_factor-analysis.sh`, you should be able to see:
```
------ Figure 5(a,b) Evaluation Start ------
make: Nothing to be done for 'dbtest'.
------ Make Done ------
tpc-c figure5(a) high contention [occ policy]
tpc-c. setting=[occ policy]. num-threads=48, warehouse-num=1
RESULT throughput(69264.8),agg_abort_rate(0.227792)
tpc-c. setting=[occ policy]. num-threads=48, warehouse-num=1
RESULT throughput(68717.2),agg_abort_rate(0.233193)
tpc-c. setting=[occ policy]. num-threads=48, warehouse-num=1
RESULT throughput(68065.9),agg_abort_rate(0.24509)
tpc-c. setting=[occ policy]. num-threads=48, warehouse-num=1
RESULT throughput(68285),agg_abort_rate(0.23205)
tpc-c. setting=[occ policy]. num-threads=48, warehouse-num=1
RESULT throughput(66486.2),agg_abort_rate(0.225003)
...
```

**TPC-C latency**
After executing `./ae_tpcc_latency.sh`, you should be able to see:
```
------ Table 2 Evaluation Start ------
make: Nothing to be done for 'dbtest'.
------ Make Done ------
tpc-c Table 2 Polyjuice
tpc-c. cc=Polyjuice. num-threads=48, warehouse-num=1
Latency - microseconds(µs)
new_order_p50 latency - 155
new_order_p90 latency - 187
new_order_p99 latency - 270
payment_p50 latency - 156
payment_p90 latency - 189
payment_p99 latency - 280
delivery_p50 latency - 139
delivery_p90 latency - 409
delivery_p99 latency - 775
...
```

**TPC-E performance**
After executing `./ae_tpce_performance.sh`, you should be able to see:
```
------ Figure 7(a) Evaluation Start ------
make: Nothing to be done for 'dbtest'.
------ Make Done ------
tpc-e figure7(a) Polyjuice
tpc-e. cc=Polyjuice. num-threads=48, zipf_theta=1.0
RESULT throughput(1.02724e+06),agg_abort_rate(0.061197)
tpc-e. cc=Polyjuice. num-threads=48, zipf_theta=1.0
RESULT throughput(1.00206e+06),agg_abort_rate(0.0609897)
tpc-e. cc=Polyjuice. num-threads=48, zipf_theta=1.0
RESULT throughput(1.02596e+06),agg_abort_rate(0.0618323)
tpc-e. cc=Polyjuice. num-threads=48, zipf_theta=1.0
RESULT throughput(1.02768e+06),agg_abort_rate(0.061071)
tpc-e. cc=Polyjuice. num-threads=48, zipf_theta=1.0
RESULT throughput(1.00391e+06),agg_abort_rate(0.0615438)
...
```

**TPC-E scalability**
After executing `./ae_tpce_scalability.sh`, you should be able to see:
```
------ Figure 7(b) Evaluation Start ------
make: Nothing to be done for 'dbtest'.
------ Make Done ------
tpc-e figure7(b) Polyjuice
tpc-e. cc=Polyjuice. num-threads=1, zipf_theta=3.0
RESULT throughput(33598.3),agg_abort_rate(0.0380638)
tpc-e. cc=Polyjuice. num-threads=1, zipf_theta=3.0
RESULT throughput(33660.7),agg_abort_rate(0.0380675)
tpc-e. cc=Polyjuice. num-threads=1, zipf_theta=3.0
RESULT throughput(31646),agg_abort_rate(0.0380343)
tpc-e. cc=Polyjuice. num-threads=1, zipf_theta=3.0
RESULT throughput(33687.7),agg_abort_rate(0.0380684)
tpc-e. cc=Polyjuice. num-threads=1, zipf_theta=3.0
RESULT throughput(30935.8),agg_abort_rate(0.0380866)
...
```

**Micro performance**
After executing `./ae_micro_performance.sh`, you should be able to see:
```
------ Figure 9 Evaluation Start ------
make: Nothing to be done for 'dbtest'.
------ Make Done ------
micro-benchmark(10 tx type) figure9 Polyjuice
micro-benchmark(10 tx type). cc=Polyjuice. num-threads=48, zipf_theta=0.4
RESULT throughput(857784),agg_abort_rate(0.0178433)
micro-benchmark(10 tx type). cc=Polyjuice. num-threads=48, zipf_theta=0.4
RESULT throughput(828046),agg_abort_rate(0.0178269)
micro-benchmark(10 tx type). cc=Polyjuice. num-threads=48, zipf_theta=0.4
RESULT throughput(830430),agg_abort_rate(0.0178426)
micro-benchmark(10 tx type). cc=Polyjuice. num-threads=48, zipf_theta=0.4
RESULT throughput(811564),agg_abort_rate(0.0178326)
micro-benchmark(10 tx type). cc=Polyjuice. num-threads=48, zipf_theta=0.4
RESULT throughput(831768),agg_abort_rate(0.0179003)
...
```

**TPC-C different workloads performance**
After executing `./ae_tpcc_different_workloads.sh`, you should be able to see:
```
------ Figure 10 Evaluation Start ------
make: Nothing to be done for 'dbtest'.
------ Make Done ------
tpc-c figure10 Polyjuice
tpc-c. cc=Polyjuice. num-threads=48, warehouse-num=1
RESULT throughput(304057),agg_abort_rate(0.0286652)
tpc-c. cc=Polyjuice. num-threads=48, warehouse-num=1
RESULT throughput(289308),agg_abort_rate(0.0280998)
tpc-c. cc=Polyjuice. num-threads=48, warehouse-num=1
RESULT throughput(300991),agg_abort_rate(0.0294829)
tpc-c. cc=Polyjuice. num-threads=48, warehouse-num=1
RESULT throughput(284700),agg_abort_rate(0.0282824)
tpc-c. cc=Polyjuice. num-threads=48, warehouse-num=1
RESULT throughput(300755),agg_abort_rate(0.0306853)
...
```

#### 2.4.2 Training Output
There are two ways to see the training results:
1. The training information will be directly output on the terminal, including each policy's performance within every training iteration.
2. Each training process creates a folder to store results under corresponding_folder/training/saved_model, named by experiment name and a timestamp.
You can collect the result folders together and use tensorboard to view the training curves visually as in the paper.
```
tensorboard --logdir="./"
```

Experiments related w/ training:

| Experiment | Script | Corresponding Folder | Figure
| ---- | --- | --- | --- |
| polyjuice EA | ae_ea_rl.sh | ae-tpcc-polyjuice | Figure 5
| polyjuice RL | ae_ea_rl.sh | ae-tpcc-polyjuice-rl | Figure 5
 </br>

For example, if you run
```
./ae_ea_rl.sh
```
It will create new folders, which are used for saving the training logs, in the following two directories:
```
ae-tpcc-polyjuice/training/saved_model/
ae-tpcc-polyjuice-rl/training/saved_model/
```
After the training, you can `cd` into corresponding folders and get the training logs.


# Publication
[Polyjuice: High-Performance Transactions via Learned Concurrency Control](https://www.usenix.org/system/files/osdi21-wang-jiachen.pdf).

Jiachen Wang*, Ding Ding*, Huan Wang, Conrad Christensen, Zhaoguo Wang, Haibo Chen, and Jinyang Li. 

Operating Systems Design and Implementation (OSDI), 2021