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

#include "../amd64.h"
#include "../txn.h"
#include "../macros.h"
#include "../scopedperf.hh"
#include "../spinlock.h"
#include "../mcs_spinlock.h"

#include "bench.h"
#include "ndb_wrapper.h"
#include "ndb_wrapper_impl.h"
#include "micro_lock_perf.h"

#define MAX_UNIT_NUM 1024

using namespace std;
using namespace util;

static const size_t TESTRecordSize = 64;

static size_t unit_cycle = 3000;
static size_t unit_num = 32;

static uint32_t num_ctn = 100;
static uint32_t f_ctn = 100;



inline void spin(uint64_t cycles)
{
  uint64_t start = rdtsc();
  while((rdtsc() - start) < cycles);
}


struct status
{
  volatile uint32_t step;
  
  status():step(0){}

};

#ifdef MCSLOCK

struct lock_s {

  mcslock l_;

  void lock(void *p)
  { 
    l_.acquire((mcslock::qnode_t *)p);
  }

  void unlock(void *p)
  {
    l_.release((mcslock::qnode_t *)p);
  }

}CACHE_ALIGNED;

#else  

struct lock_s {

  spinlock l_;

  void lock(void *param)
  { 
    l_.lock();
  }

  void unlock(void *param)
  {
    l_.unlock();
  }

}CACHE_ALIGNED;

#endif

static lock_s locks[MAX_UNIT_NUM];
static volatile status* volatile stats[MAX_UNIT_NUM];

class lock_perf_worker : public bench_worker {
public:
  lock_perf_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("TESTTABLE")),
      computation_n(0)
  {}

  txn_result
  txn_lock_perf()
  {
    uint32_t wid = worker_id > 64 ? worker_id - 1: worker_id;

    if(stats[worker_id] == nullptr)
	     stats[worker_id] = new status();

    for(uint32_t i = 0; i < unit_num; i++)
    {
      
#ifdef WAITSYNC
      if(wid != worker_id) {
        while(stats[wid] == nullptr 
		|| stats[wid]->step < (stats[worker_id]->step + 1))
        {
          nop_pause();
        }
      }
#endif

#ifdef SINGLECTS
      if(i > 0) {
        spin(unit_cycle);
        stats[worker_id]->step++;
        continue;
      }
#endif      

#ifdef MCSLOCK
      mcslock::qnode_t n;
      locks[i].lock((void *)&n);
#else      
      locks[i].lock(nullptr);
#endif

      spin(unit_cycle);

#ifdef MCSLOCK
      locks[i].unlock((void *)&n);
#else      
      locks[i].unlock(nullptr);
#endif      

      stats[worker_id]->step++;
    }

    return txn_result(true, 0);
  }

  static txn_result
  TxnLockPerf(bench_worker *w)
  {
    return static_cast<lock_perf_worker *>(w)->txn_lock_perf();
  }

  virtual workload_desc_vec
  get_workload() const
  {
    workload_desc_vec w;
    w.push_back(workload_desc("Lock Perf",  1.0, TxnLockPerf));
    
    return w;
  }

protected:

  virtual void
  on_run_setup() OVERRIDE
  {}

  inline ALWAYS_INLINE string &
  str() {
    return *arena.next();
  }

private:
  abstract_ordered_index *tbl;

  string obj_key0;
  string obj_key1;
  string obj_v;

  uint64_t computation_n;
};

class testtable_loader : public bench_loader {

public:
  testtable_loader(unsigned long seed,
                        abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
    : bench_loader(seed, db, open_tables)
  {}

protected:
  virtual void
  load()
  {
    printf("Load Done\n");
  }

};



class lock_perf_bench_runner : public bench_runner {
public:
  lock_perf_bench_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["TESTTABLE"] = db->open_index("TESTTABLE", TESTRecordSize);
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new testtable_loader(0, db, open_tables));
   
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
        new lock_perf_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }

};

void
micro_lock_perf_test(abstract_db *db, int argc, char **argv)
{
  printf("Lock Perf Test f_ctn %d num_ctn %d\n", f_ctn, num_ctn);
  
  lock_perf_bench_runner r(db);

  r.run();
}
