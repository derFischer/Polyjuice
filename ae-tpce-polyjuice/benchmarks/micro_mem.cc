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

static const size_t TESTSTEP = 99;
static const size_t TESTKeySizePerStep = 1;

static const size_t TESTRecordSize = TESTSTEP * TESTKeySizePerStep;

static atomic<int64_t> result;
static atomic<int64_t> failed;

static conflict_graph* cgraph = nullptr;
enum  MICRO_BADCOUNT_TYPES
{
  BADCOUNT = 1,
};

class test_mem_loader : public bench_loader {

public:
  test_mem_loader(unsigned long seed,
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
    
retry:
      
      try {

        db->one_op_begin(txn);  

        for (size_t i = 1; i <= TESTRecordSize; i++) {
          const test::key k(i);
          const test::value v(100, 100);
          tbl->insert(txn, Encode(k), Encode(obj_buf, v));
        }


        for (size_t i = 1; i <= TESTRecordSize; i++) {
          const test::key k(i);
          const test::value v(i, i * i);
          tbl->put(txn, Encode(k), Encode(obj_buf, v));
        }

       bool rp = db->one_op_end(txn);

        if(!rp) {
          goto retry;
        }

      } catch (piece_abort_exception &ex) {
        goto retry;
      }

      bool res = db->commit_txn(txn);
      ALWAYS_ASSERT(res);

    } catch (abstract_db::abstract_abort_exception &ex) {
      ALWAYS_ASSERT(false);
      db->abort_txn(txn);
    }

    void * const txn2 = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);
    try {
    
      for (uint32_t i = 1; i <= TESTRecordSize; i++) {

        db->one_op_begin(txn2);
        const test::key k(i);
        string test_v;
        ALWAYS_ASSERT(tbl->get(txn2, Encode(k), test_v));
        
        test::value temp; 
        const test::value *v = Decode(test_v, temp);
        
        //fprintf(stderr, "%d\n", v->t_v_count);
        ALWAYS_ASSERT(v->t_v_count == (int32_t)i);
        ALWAYS_ASSERT(v->t_v_tid == (i * i));
        
        bool rp = db->one_op_end(txn2);
        ALWAYS_ASSERT(rp);
      }

      bool res = db->commit_txn(txn2);
      ALWAYS_ASSERT(res);

    } catch (abstract_db::abstract_abort_exception &ex) {
      ALWAYS_ASSERT(false);
      db->abort_txn(txn2);
    }

    printf("Load Done\n");
  }

};


class micro_mem_worker : public bench_worker {
public:
  micro_mem_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("TESTTABLE"))
  {}

  static txn_result
  TxnMem(bench_worker *w)
  {
    return static_cast<micro_mem_worker *>(w)->txn_mem();
  }


  txn_result
  txn_mem()
  {
    
    return txn_result(true, 0);
  }

  
  virtual workload_desc_vec
  get_workload() const
  {
  
    workload_desc_vec w;
    w.push_back(workload_desc("Micro Mem",  1.0, TxnMem));
    
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
};



class micro_mem_runner : public bench_runner {
public:
  micro_mem_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["TESTTABLE"] = db->open_index("TESTTABLE", TESTRecordSize);
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new test_mem_loader(0, db, open_tables));
   
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
        new micro_mem_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }



  virtual std::vector<bench_checker*> make_checkers()
  {
    return vector<bench_checker*>();
  }

};

void
micro_do_mem_test(abstract_db *db, int argc, char **argv)
{
  printf("Hello World \n");
  cgraph = new conflict_graph(TXTTPES);
  cgraph->init_txn(BADCOUNT, TESTSTEP);
  micro_mem_runner r(db);

  r.run();
}
