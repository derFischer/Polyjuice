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

#include "micro_badcount.h"
#include "bench.h"
#include "ndb_wrapper.h"

using namespace std;
using namespace util;

#define TEST_TABLE_LIST(x) \
  x(test) 


static const size_t INITIALSize = 0;
static const size_t INSERTKeyNum = 16;
static const size_t INSERTStep = 1;

static atomic<int64_t> result;
static atomic<int64_t> failed;

class micro_insert_worker : public bench_worker {
public:
  micro_insert_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("TESTTABLE"))
  {}

  static txn_result
  TxnInsert(bench_worker *w)
  {
    return static_cast<micro_insert_worker *>(w)->txn_insert();
  }

  txn_result
  txn_insert()
  {
    string obj_buf;
    bool res = false;
    scoped_str_arena s_arena(arena);
    void * const txn = db->new_txn(txn_flags, arena, txn_buf());

    for(size_t step = 0; step < INSERTStep; step++) {

        bool last_is_insert = false;
        int64_t last_v = 0;

        for (size_t i = 1; i < INSERTKeyNum; i++) {

          const test::key k(i + step * INSERTKeyNum);
          string test_v;

          test::value v;
          v.t_v_count = worker_id;

          if(!tbl->get(txn, Encode(k), test_v)) {

            if(i != 1) {
              ALWAYS_ASSERT(last_is_insert);
            }
            last_is_insert = true;

            tbl->insert(txn, Encode(k), Encode(str(), v));
          }
          else {

            test::value temp; 
            const test::value *tmp_v = Decode(test_v, temp);

            if(i != 1) {
              ALWAYS_ASSERT(!last_is_insert);
              ALWAYS_ASSERT(tmp_v->t_v_count == last_v);
            }

            last_is_insert = false;
            last_v = tmp_v->t_v_count;
            
            tbl->put(txn, Encode(k), Encode(str(), v));
          }

        }
    }

    res = db->commit_txn(txn);


    if(res) {
        result++;
    }
    else {
      fprintf(stderr, "Failed\n");
        failed++;      
    } 
    
    return txn_result(res, 0);
  }

  
  virtual workload_desc_vec
  get_workload() const
  {
  
    workload_desc_vec w;
    w.push_back(workload_desc("Micro Insert",  1.0, TxnInsert));
    
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

class micro_insert_loader : public bench_loader {

public:
  micro_insert_loader(unsigned long seed,
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
    void * const txn = db->new_txn(txn_flags, arena, txn_buf());
    try {
    
      for (size_t i = 1; i <= INITIALSize; i++) {
        const test::key k(i);
        const test::value v(0);
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





class micro_insert_runner : public bench_runner {
public:
  micro_insert_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["TESTTABLE"] = db->open_index("TESTTABLE", INITIALSize);
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new micro_insert_loader(0, db, open_tables));
   
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
        new micro_insert_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }


};

void
micro_insert_do_test(abstract_db *db, int argc, char **argv)
{
  printf("Hello World\n");
  micro_insert_runner r(db);

  r.run();
}
