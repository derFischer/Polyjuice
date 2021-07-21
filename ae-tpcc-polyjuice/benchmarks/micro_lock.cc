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
#include "../spinlock-xchg.h"

#include "bench.h"
#include "micro_badcount.h"
#include "ndb_wrapper.h"
#include "ndb_wrapper_impl.h"

using namespace std;
using namespace util;


#define SPINLOCKRANGE 32

#define PINCPU

#define TEST_TABLE_LIST(x) \
  x(test) 


static const size_t TXTTPES = 1;

static const size_t records_per_table = 10000;

static unsigned g_txn_workload_mix[] = {100};

static conflict_graph* cgraph = nullptr;

#define CACHELINE 64

char padding[CACHELINE];
static mcslock mlock;
char padding2[CACHELINE];


spinlockxg slock[64];

enum  MICRO_TYPES
{
  MICROBENCH = 1,
};


static inline void 
bind_thread(uint worker_id)
{

#ifdef PINCPU
  const size_t cpu = worker_id % coreid::num_cpus_online();
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  int s = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (s != 0)
      fprintf(stderr, "pthread_setaffinity_np");
#endif

}


class lock_worker : public bench_worker {
public:
  lock_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("TESTTABLE")),
      computation_n(0)
  {}

  txn_result
  txn_lock()
  {

    string obj_buf;
    bool res = false;

    scoped_str_arena s_arena(arena);
    void * txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);
    db->init_txn(txn, cgraph, MICROBENCH);

    

    try {
          // slock[worker_id % SPINLOCKRANGE].spin_lock();

          mcslock::qnode_t node;
          mlock.acquire(&node);
          const test::key k(1);
          string test_v;

          ALWAYS_ASSERT(tbl->get(txn, Encode(k), test_v));

          test::value temp; 
          const test::value *v = Decode(test_v, temp);

          test::value v_new(*v);
          v_new.t_v_count++;   
          ALWAYS_ASSERT(v_new.t_v_count > 0);
          tbl->put(txn, Encode(str(), k), Encode(obj_v, v_new));

          res = db->commit_txn(txn);

          mlock.release(&node);

          // slock[worker_id % SPINLOCKRANGE].spin_unlock();

      } catch (abstract_db::abstract_abort_exception &ex) {
  
        db->abort_txn(txn);
      }

      txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);
      db->init_txn(txn, cgraph, MICROBENCH);
      try {

          for(int i = 0; i < 16; i ++) {

            const test::key k(100);
            string test_v;

            ALWAYS_ASSERT(tbl->get(txn, Encode(k), test_v));

            test::value temp; 
            const test::value *v = Decode(test_v, temp);

            test::value v_new(*v);
            v_new.t_v_count++;   
            ALWAYS_ASSERT(v_new.t_v_count > 0);
            tbl->put(txn, Encode(str(), k), Encode(obj_v, v_new));
          }
          res = db->commit_txn(txn);


      } catch (abstract_db::abstract_abort_exception &ex) {
  
        db->abort_txn(txn);
      }


    return txn_result(res, 0);
  }

  static txn_result
  TxnMicro(bench_worker *w)
  {
    return static_cast<lock_worker *>(w)->txn_lock();
  }

  
  virtual workload_desc_vec
  get_workload() const
  {
  
    workload_desc_vec w;
    unsigned m = 0;
    for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
      m += g_txn_workload_mix[i];
    ALWAYS_ASSERT(m == 100);
    if (g_txn_workload_mix[0])
      w.push_back(workload_desc("Micro bench",  double(g_txn_workload_mix[0])/100.0, TxnMicro));
    
    return w;
  }

protected:

  virtual void
  on_run_setup() OVERRIDE
  {
    bind_thread(worker_id);
  }

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

class lockbench_loader : public bench_loader {

public:
  lockbench_loader(unsigned long seed,
                        abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
    : bench_loader(seed, db, open_tables)
  {}

protected:
  virtual void
  load()
  {

    abstract_ordered_index *tbl = open_tables.at("TESTTABLE");
    string obj_buf;

    for (size_t i = 0; i < records_per_table; i++) {

      scoped_str_arena s_arena(arena);
      void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);

      try {
  retry:
        try {

          
            const test::key k(i);
            const test::value v(0, 0);
            tbl->insert(txn, Encode(k), Encode(obj_buf, v));

        } catch (piece_abort_exception &ex) {
          goto retry;
        }

        bool res = db->commit_txn(txn);
        ALWAYS_ASSERT(res);

      } catch (abstract_db::abstract_abort_exception &ex) {
        ALWAYS_ASSERT(false);
        db->abort_txn(txn);
      }
    }

    printf("Load Done\n");
  }

};





class lockbench_checker : public bench_checker {

public:
  lockbench_checker(abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
    : bench_checker(db, open_tables)
  {}

protected:
  virtual void
  check()
  {}
};



class lock_bench_runner : public bench_runner {
public:
  lock_bench_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["TESTTABLE"] = db->open_index("TESTTABLE", (records_per_table));
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new lockbench_loader(0, db, open_tables));
   
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
        new lock_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }



  virtual std::vector<bench_checker*> make_checkers()
  {
    std::vector<bench_checker*>* bw = new std::vector<bench_checker*>();
    bw->push_back(new lockbench_checker(db, open_tables));

    return *bw;
  }

};

void
lockbench_do_test(abstract_db *db, int argc, char **argv)
{

  printf("Run Lock Benchmark %d %s\n", argc, *argv);

  cgraph = new conflict_graph(TXTTPES);
  cgraph->init_txn(MICROBENCH, 1);
  lock_bench_runner r(db);

  r.run();

}
