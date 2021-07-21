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

#include "bench.h"
#include "micro_badcount.h"
#include "ndb_wrapper.h"
 #include "ndb_wrapper_impl.h"

using namespace std;
using namespace util;

#define TEST_TABLE_LIST(x) \
  x(test) 

static const size_t TXTTPES = 1;

static const size_t NRECORDS = 100;

static unsigned g_txn_workload_mix[] = {100};


class test_worker : public bench_worker {
public:
  test_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("TESTTABLE")),
      computation_n(0)
  {}

  txn_result
  txn_badcount()
  {

    string obj_buf;
    bool res = false;
    scoped_str_arena s_arena(arena);
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);
    int64_t check_id = 0;
    uint32_t readonly = (worker_id % 2);

    //piece_abort_exceptiontf("READ WRITE TXN\n");  
    try {

          
          for (size_t s = 0; s < NRECORDS; s++) {

              const test::key k(s);
              string test_v;

              if(readonly)
                ALWAYS_ASSERT(tbl->get_for_read(txn, Encode(k), test_v));
              else
                ALWAYS_ASSERT(tbl->get(txn, Encode(k), test_v));
              
              test::value temp; 
              const test::value *v = Decode(test_v, temp);
              
              test::value v_new(*v);
              v_new.t_v_count++;
              ALWAYS_ASSERT(v_new.t_v_count > 0);
              
              if(s == 0) {
                check_id = v_new.t_v_tid;
              } else {
                if(check_id != v_new.t_v_tid) {
                  fprintf(stderr, "ERROR %ld -- %ld\n", check_id, v_new.t_v_tid);
                }
                ALWAYS_ASSERT(check_id == v_new.t_v_tid);
              }

              v_new.t_v_tid = worker_id;
              
              if(!readonly)
                tbl->put(txn, Encode(str(), k), Encode(obj_v, v_new));
              
          }
  
        res = db->commit_txn(txn);
        ALWAYS_ASSERT(res);

      } catch (abstract_db::abstract_abort_exception &ex) {
        ALWAYS_ASSERT(false);
        db->abort_txn(txn);
      }

    return txn_result(res, 0);
  }

  static txn_result
  TxnBadcount(bench_worker *w)
  {
    return static_cast<test_worker *>(w)->txn_badcount();
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
      w.push_back(workload_desc("Bad Count",  double(g_txn_workload_mix[0])/100.0, TxnBadcount));
    
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

    abstract_ordered_index *tbl = open_tables.at("TESTTABLE");
    string obj_buf;

    scoped_str_arena s_arena(arena);
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);
    try {
    
      for (size_t i = 0; i < NRECORDS; i++) {
        const test::key k(i);
        const test::value v(0, 0);
        tbl->insert(txn, Encode(k), Encode(obj_buf, v));
      }


      bool res = db->commit_txn(txn);
      ALWAYS_ASSERT(res);

    } catch (abstract_db::abstract_abort_exception &ex) {
      ALWAYS_ASSERT(false);
      db->abort_txn(txn);
    }

    printf("Load Done\n");
  }

};


class micro_badcount_runner : public bench_runner {
public:
  micro_badcount_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["TESTTABLE"] = db->open_index("TESTTABLE", NRECORDS);
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
        new test_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }

};

void
micro_badcount_do_test(abstract_db *db, int argc, char **argv)
{
  printf("Micro Bad Count\n");
  micro_badcount_runner r(db);

  r.run();
}
