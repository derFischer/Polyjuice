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

static const size_t INSERTKeyNum =10;
static const size_t INSERTStep = 1;  //This micro bench can not be used for interleaving constrained

static atomic<int64_t> result;
static atomic<int64_t> failed;

static const size_t TXTYPES = 1;
static conflict_graph* cgraph = nullptr;
enum MICRO_RANGE_TYPES
{
  MICRORANGE = 1,
};


class micro_scan_callback : public abstract_ordered_index::scan_callback {

public:
  
  micro_scan_callback() : k_no(0) {}
  
  virtual bool invoke(
      const char *keyp, size_t keylen,
      const string &value)
  {
    k_no = const_cast<test::key*>(Decode(keyp, k_no_temp));
    kv.emplace_back(*k_no);
    //fprintf(stderr, "Get %d\n", k_no->t_k_id);
    return true;
  }

  inline std::vector<test::key>&
  get_vector()
  {
    return kv;
  }

private:
  test::key k_no_temp;
  test::key *k_no;
  std::vector<test::key> kv;
  
};

class micro_range_worker : public bench_worker {
public:
  micro_range_worker(unsigned int worker_id,
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
    return static_cast<micro_range_worker *>(w)->txn_range();
  }


  bool 
  check(std::vector<test::key>& keys)
  {

    uint64_t last = keys[0].t_k_id;

    for(unsigned i = 1; i < keys.size(); i++) {
      uint64_t cur = keys[i].t_k_id;
      if(cur != (last + 2)) {
        //fprintf(stderr, "[CHECK ERROR] last %ld cur %ld\n", last, cur);
        return false;
      }
      last = cur;
    }
    return true;
  }


  txn_result
  txn_range()
  {
    string obj_buf;
    bool res = false;
    std::vector<test::key> kv;

    scoped_str_arena s_arena(arena);
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);
      
    db->init_txn(txn, cgraph, MICRORANGE);

    for(size_t step = 0; step < INSERTStep; step++) {

retry:
      try {

        

        micro_scan_callback c_scan;
        const test::key k_0(1 + step * INSERTKeyNum);
        const test::key k_1((step + 1) * INSERTKeyNum);

        tbl->scan(txn, Encode(obj_key0, k_0), &Encode(obj_key1, k_1), c_scan, s_arena.get());

        kv = c_scan.get_vector();
#if 0
        for(unsigned i = 0; i < kv.size(); i++) {
          fprintf(stderr, "key %ld\n", kv[i].t_k_id);
        }
#endif
        if(kv.size() > 0 ){

          uint64_t initial = kv[0].t_k_id;

          if(initial % 2 == 0)
            initial--;
          else
            initial++;

          for (size_t i = initial; i < (step + 1) * INSERTKeyNum; i+=2) {

            const test::key k(i);
            test::value v;
            v.t_v_count = worker_id;
            v.t_v_tid = db->get_tid(txn);
            
            //fprintf(stderr, "Insert %ld\n", k.t_k_id);
          
            tbl->insert(txn, Encode(str(), k), Encode(obj_v, v));
          }

          for (size_t i = 0; i < kv.size(); i++) {
            const test::key k(kv[i].t_k_id);
            //fprintf(stderr, "Remove %ld\n", k.t_k_id);
            tbl->remove(txn, Encode(str(), k));
          }

        }

        
      } catch(piece_abort_exception &ex) {
        
        //fprintf(stderr, " %ld--%ld Exception Retry\n", worker_id,  coreid::core_id()); 
        goto retry;
      }

  }

  res = db->commit_txn(txn);


  if(res) {
      result++;
      ALWAYS_ASSERT(kv.size());
      ALWAYS_ASSERT(check(kv));
      //check(resv, ins);
      //fprintf(stderr, "Txn Succ\n"); 
  }
  else {
      failed++;      
      //fprintf(stderr, "Txn Abort\n"); 
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

class micro_range_loader : public bench_loader {

public:
  micro_range_loader(unsigned long seed,
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
    
        

      for (size_t i = 0; i < (INSERTKeyNum * INSERTStep); i++) {
        if(i % 2 == 0)
          continue;
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




class micro_range_runner : public bench_runner {
public:
  micro_range_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["TESTTABLE"] = db->open_index("TESTTABLE", INSERTKeyNum);
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new micro_range_loader(0, db, open_tables));
   
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
        new micro_range_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }


};

void
micro_range_do_test(abstract_db *db, int argc, char **argv)
{
  printf("Hello World\n");

  cgraph = new conflict_graph(TXTYPES);
  cgraph->init_txn(MICRORANGE, INSERTStep);

  micro_range_runner r(db);

  r.run();
}
