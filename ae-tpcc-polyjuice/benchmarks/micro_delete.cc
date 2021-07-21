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
static const size_t INSERTKeyNum =1;
static const size_t DELETEStep = 10;

static atomic<int64_t> result;
static atomic<int64_t> failed;

static const size_t TXTYPES = 1;
static conflict_graph* cgraph = nullptr;
enum MICRO_DELETE_TYPES
{
  MICRODELETE = 1,
};

class micro_delete_worker : public bench_worker {
public:
  micro_delete_worker(unsigned int worker_id,
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
    return static_cast<micro_delete_worker *>(w)->txn_delete();
  }


  bool 
  check(int64_t resv[], bool ins[])
  {
    for(size_t step = 0; step < DELETEStep; step++)
    {
      for(size_t i = 1; i < INSERTKeyNum; i++) {

        ALWAYS_ASSERT(ins[i] == ins[0]);
        ALWAYS_ASSERT(resv[i] == resv[0]);

      }
    }
    return true;
  }


  txn_result
  txn_delete()
  {
    string obj_buf;
    bool res = false;
    scoped_str_arena s_arena(arena);

    //fprintf(stderr, "Begin TX\n");

    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);

    db->init_txn(txn, cgraph, MICRODELETE);

    bool ins[INSERTKeyNum*DELETEStep];
    int64_t resv[INSERTKeyNum*DELETEStep];

    for(size_t step = 0; step < DELETEStep; step++) {

retry:
      try {

        //fprintf(stderr, "Begin Piece\n");
        //db->one_op_begin(txn);     
        db->mul_ops_begin(txn);     

        for (size_t i = 0; i < INSERTKeyNum; i++) {

          const test::key k(i + step * INSERTKeyNum);
          //fprintf(stderr, "Insert %ld\n", tmp + step * INSERTKeyNum);
          string test_v;

          test::value v;
          v.t_v_count = worker_id;
          v.t_v_tid = db->get_tid(txn);

          //fprintf(stderr, "Get %d\n", i + step * INSERTKeyNum);

          if(!tbl->get(txn, Encode(k), test_v)) {

            ins[i + step * INSERTKeyNum] = false;
            resv[i + step * INSERTKeyNum] = 0;

            //fprintf(stderr, "Insert %d\n", i);

            tbl->insert(txn, Encode(str(), k), Encode(str(), v));
          }
          else {

            test::value temp; 
            const test::value *tmp_v = Decode(test_v, temp);
            
            ins[i + step * INSERTKeyNum] = true;
            resv[i + step * INSERTKeyNum] = tmp_v->t_v_tid;
            
            //fprintf(stderr, "Remove %d\n", i + step * INSERTKeyNum);

            tbl->remove(txn, Encode(str(), k));
          }

        }

        bool rp = db->mul_ops_end(txn);
        //bool rp = db->one_op_end(txn);

        if(!rp) {
          //fprintf(stderr, " %ld--%ld Normal Retry\n", worker_id,  coreid::core_id()); 
          goto retry;
        }

      } catch(piece_abort_exception &ex) {
        //TODO Abort Piece
         //fprintf(stderr, " %ld--%ld Exception Retry\n", worker_id,  coreid::core_id()); 
        db->atomic_ops_abort(txn);
        goto retry;
      }

  }

  res = db->commit_txn(txn);


  if(res) {
      result++;
      check(resv, ins);
  }
  else {
      ALWAYS_ASSERT(false);
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

class micro_delete_loader : public bench_loader {

public:
  micro_delete_loader(unsigned long seed,
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





class micro_delete_checker : public bench_checker {

public:
  micro_delete_checker(abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
    : bench_checker(db, open_tables)
  {}

protected:
  virtual void
  check()
  {

    abstract_ordered_index *tbl = open_tables.at("TESTTABLE");
    string obj_buf;

    
    scoped_str_arena s_arena(arena);
    void * const txn = db->new_txn(txn_flags, arena, txn_buf());

    try {

      for (size_t i = 1; i <= INSERTKeyNum * DELETEStep; i++) {
        const test::key k(i);
        string test_v;
        ALWAYS_ASSERT(tbl->get(txn, Encode(k), test_v));
        test::value temp; 
        const test::value *v = Decode(test_v, temp);
        int64_t val = v->t_v_count;
        //if(val != )
        printf("val %ld result %ld failed times %ld\n", val, result.load(), failed.load());
        ALWAYS_ASSERT(val == result);
      }

      bool res = db->commit_txn(txn);
      ALWAYS_ASSERT(res);

    } catch (abstract_db::abstract_abort_exception &ex) {
      ALWAYS_ASSERT(false);
      db->abort_txn(txn);
    }
  }
};



class micro_delete_runner : public bench_runner {
public:
  micro_delete_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["TESTTABLE"] = db->open_index("TESTTABLE", INITIALSize);
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new micro_delete_loader(0, db, open_tables));
   
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
        new micro_delete_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }



  virtual std::vector<bench_checker*> make_checkers()
  {
    std::vector<bench_checker*>* bw = new std::vector<bench_checker*>();
    //bw->push_back(new micro_delete_checker(db, open_tables));

    return *bw;
  }

};

void
micro_delete_do_test(abstract_db *db, int argc, char **argv)
{
  printf("Hello World\n");

  cgraph = new conflict_graph(TXTYPES);
  cgraph->init_txn(MICRODELETE, DELETEStep);

  micro_delete_runner r(db);

  r.run();
}
