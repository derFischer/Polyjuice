Silo
=====

This project contains the prototype of the database system described in 

    Speedy Transactions in Multicore In-Memory Databases 
    Stephen Tu, Wenting Zheng, Eddie Kohler, Barbara Liskov, Samuel Madden 
    To appear in SOSP 2013. 
    http://people.csail.mit.edu/stephentu/papers/silo.pdf

This code is an ongoing work in progress.

Build
-----

There are several options to build. `MODE` is an important variable
governing the type of build. The default is `MODE=perf`, see the
Makefile for more options. `DEBUG=1` triggers a debug build (off by
default). `CHECK_INVARIANTS=1` enables invariant checking. There are
two targets: the default target which builds the test suite, and
`dbtest` which builds the benchmark suite. Examples:

    MODE=perf DEBUG=1 CHECK_INVARIANTS=1 make -j
    MODE=perf make -j dbtest

Each different combination of `MODE`, `DEBUG`, and `CHECK_INVARIANTS` triggers
a unique output directory; for example, the first command above builds to
`out-perf.debug.check.masstree`.

Silo now uses [Masstree](https://github.com/kohler/masstree-beta) by default as
the default index tree. To use the old tree, set `MASSTREE=0`.

ChamCC Usage
-------

To build ChamCC, example:
```
    make clean
    make dbtest -j
```
TPC-C benchmark running:

Option|Comment
---|---
--bench|which benchmark to run
--num-threads|number of workers
--runtime|running time (seconds)
--retry-aborted-transactions|whether to retry aborted transactions
--bench-opt|workload settings e.g.mix percentage of tpc-c
--scale-factor|number of warehosues
--db-type|db type, "ndb-ic3" for ChamCC
--policy|which policy to use (occ, ic3 or a location of a policy file), occ and ic3 are const manual policies
--consistency-check|whether to do consistency test after benchmark running

To run benchmarks, example:
```
    ./out-perf.masstree/benchmarks/dbtest --bench tpcc --num-threads 1  --runtime 5 --retry-aborted-transactions --bench-opt "--workload-mix 100,0,0,0,0" --scale-factor 1 --db-type ndb-ic3 --policy occ --consistency-check
```

ChamCC Code
-------
**ChamCC access action support(get, put, update):**  
**piece_impl.h**, there are serval piece operations in this file (the name piece operation is inherited from IC3 code), the only operation we use in ChamCC is mix_op (mix operation), which is just a execution agent.  
Get action logic are in mix_op::do_get().  
Put action logic are in mix_op::do_put().  

**ChamCC wait action support(noWait, waitCommit, waitAccess):**   
**txn_impl.h**, do_wait(). Here it handles different waiting logic. WaitAccess action requires additional information about waiting which access to finish, and this information is passed from **policy.cc**.

**ChamCC validation and commit:**  
**txn_impl.h**, which is responsible for txn operations. Commit() function is called when txn trying to commit, and the validation is commented as "Phase3" in this commit().  

**ChamCC abort:**  
**txn_impl.h**, chamcc_abort_impl(). Abort is quite complex in ChamCC, we have to get two tasks done:  
1. Unlink the access entries, which means collect back the exposed uncommitted read/write infos  
2. Cleanup inserted tuples, which means if a insert operation's tempory marker was not touched by others, we have to reclaim that marker since we are aborted  

**Policy Inference:**  
Logic related to policies are in **policy.h/policy.cc**.  

**Consistency Check:**  
**benchmarks/tpcc.cc**, class tpcc_bench_checker.  

Running
-------

To run the tests, simply invoke `<outdir>/test` with no arguments. To run the
benchmark suite, invoke `<outdir>/benchmarks/dbtest`. For now, look in
`benchmarks/dbtest.cc` for documentation on the command line arguments. An
example invocation for TPC-C is:

    <outdir>/benchmarks/dbtest \
        --verbose \
        --bench tpcc \
        --num-threads 28 \
        --scale-factor 28 \
        --runtime 30 \
        --numa-memory 112G 

Benchmarks
----------

To reproduce the graphs from the paper:

    $ cd benchmarks
    $ python runner.py /unused-dir <results-file-prefix>

If you set `DRYRUN=True` in `runner.py`, then you get to see all the
commands that would be issued by the benchmark script.
