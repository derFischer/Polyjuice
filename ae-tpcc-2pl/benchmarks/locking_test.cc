  /**
 * An implementation of TPC-C based off of:
 * https://github.com/oltpbenchmark/oltpbench/tree/master/src/com/oltpbenchmark/benchmarks/tpcc
 */

#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>
#include <malloc.h>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include <set>
#include <vector>

#include "../txn.h"
#include "../macros.h"
#include "../scopedperf.hh"
#include "../spinlock.h"

#include "../mcslock.h"

#include "bench.h"
#include "micro_badcount.h"
#include "ndb_wrapper.h"
 #include "ndb_wrapper_impl.h"

using namespace std;
using namespace util;


union data_s {

  uint64_t counter;
  char padding[CACHELINE_SIZE];

} CACHE_ALIGNED;


data_s data;

mcs_lock global_lock;

class locking_test_worker : public bench_worker {
public:
  locking_test_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b) {}



  inline void 
  speculative_exec(mcs_lock* m, mcs_lock me) {


    mcs_lock cur = me;

    mcs_lock next = cur->next;

    while(true) {

        /* No successor yet? */
        if (!next)
        {
            /* Try to atomically unlock */
            if (cmpxchg(m, cur, NULL) == cur) {

              if(cur->next != nullptr) {
                fprintf(stderr, "ERROR\n");
              }

              ALWAYS_ASSERT(cur->next == nullptr);

              return;
            } 
        
            /* Wait for successor to appear */
            while (!next) {
              cpu_relax();
              next = cur->next;
            }
        }

        data.counter++;

        cur = next;

        next = cur->next;

        cur->spec = 1;
        /* Unlock next one */

        barrier();

        cur->spin = 1;

  
        // fprintf(stderr, "unlock %lx\n", (uint64_t)cur->next);

    }

  }


  mcs_lock local_addr = nullptr;

  txn_result
  txn_locking_test()
  {

    // if(global_lock != nullptr 
    //   && global_lock == local_addr) {
    //   fprintf(stderr, "local_addr %lx global_lock %lx\n", (uint64_t)local_addr, (uint64_t)global_lock);
    //   while(true);
    // }

    while(global_lock != nullptr 
      && global_lock == local_addr) cpu_relax();

    mcs_lock_t local_node;

    if(local_addr == nullptr)
      local_addr = &local_node;
    else
      assert(local_addr == &local_node);


    lock_mcs(&global_lock, &local_node);

    

    if(local_node.spec == 0) {
      data.counter++;

      // fprintf(stderr, "unlock\n");
      // unlock_mcs(&global_lock, &local_node);

      speculative_exec(&global_lock, &local_node);
      
     } 

    return txn_result(1, 0);
  }

  static txn_result
  TxnLockingTest(bench_worker *w)
  {
    return static_cast<locking_test_worker *>(w)->txn_locking_test();
  }

  virtual workload_desc_vec
  get_workload() const
  {
  
    workload_desc_vec w;

    w.push_back(workload_desc("Locking Test",  1.0, TxnLockingTest));
    
    return w;
  }

protected:

  virtual void
  on_run_setup() OVERRIDE
  {}

};


class micro_locking_test_runner : public bench_runner {
public:
  micro_locking_test_runner(abstract_db *db)
    : bench_runner(db)
  {}

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    return ret;
  }

  virtual vector<bench_worker *>
  make_workers()
  {
    const unsigned alignment = coreid::num_cpus_online();
    const int blockstart =
      coreid::allocate_contiguous_aligned_block(nthreads, alignment);
    ALWAYS_ASSERT(blockstart >= 0);
    ALWAYS_ASSERT((blockstart % alignment) == 0);
    fast_random r(8544290);
    vector<bench_worker *> ret;
    for (size_t i = 0; i < nthreads; i++)
      ret.push_back(
        new locking_test_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }

};

void
micro_locking_do_test(abstract_db *db, int argc, char **argv)
{
  printf("Micro Locking Test\n");
  micro_locking_test_runner r(db);

  r.run();

  fprintf(stderr, "Counter %ld\n", data.counter);
}
