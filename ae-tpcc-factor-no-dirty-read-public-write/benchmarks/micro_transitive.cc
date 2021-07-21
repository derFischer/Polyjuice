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
#include <random>

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

static const size_t TXTYPES = 4;

static const size_t TESTSTEP = 100;
static const size_t TESTSTEP_PART = 40;
static const size_t TESTKeySizePerStep = 10;

static const size_t TESTRecordSize = (TESTSTEP * TESTKeySizePerStep);

static unsigned g_txn_workload_mix[] = {100};

static atomic<int64_t> result;
static atomic<int64_t> failed;
static uint64_t last_tid = 0;

static conflict_graph* cgraph = nullptr;

enum  MICRO_BADCOUNT_TYPES
{
  WRITE = 1,
  PART_WRITE,
  READ,
  PART_READ
};



class test_transitive_worker : public bench_worker {

size_t 
get_step(uint8_t type)
{
  ALWAYS_ASSERT(type > 0 && type <= PART_READ);
  if(type == WRITE || type == READ)
    return TESTSTEP;
  else
    return TESTSTEP_PART;
}

bool 
check(int64_t tids[], uint8_t type)
{
  uint32_t i = 0;

  for(;i < (TESTSTEP_PART * TESTKeySizePerStep); i++)
  {
    if(tids[i] != tids[0])
      fprintf(stderr, "%d \n",i );

    ALWAYS_ASSERT(tids[i] == tids[0]);
      //return false;
  }

  if(type == READ || type == WRITE)
  {
    //fprintf(stderr, "%d %lx %lx %lx \n",i,  tids[i + 1], tids[i], tids[0]);

    int64_t first = tids[i];

    for(;i < (TESTSTEP * TESTKeySizePerStep); i++ )
    {

      if(tids[i] != first)
        fprintf(stderr, "%d \n",i );

      ALWAYS_ASSERT(first == tids[i]);
        //return false;
    }
  }

  return true;

}


public:
  test_transitive_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("TESTTABLE")),
      computation_n(0)
  {}

  txn_result
  txn_transitive()
  {

    string obj_buf;
    bool res = false;
    scoped_str_arena s_arena(arena);
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);

    srand(db->get_tid(txn));

    uint8_t type = rand() % 4 + 1;
    size_t step = get_step(type);

    db->init_txn(txn, cgraph, type);

    //fprintf(stderr, "BEGIN type %d  step %d\n", type, step);

    int64_t tids[TESTKeySizePerStep * TESTSTEP];

    
    try {

          for (size_t s = 0; s < step; s++) {

          retry:
            try {
          
              //db->one_op_begin(txn);
              db->mul_ops_begin(txn);

              for(size_t i = 1; i <= TESTKeySizePerStep; i++) {

                size_t n = i + s * TESTKeySizePerStep;

                const test::key k(n);
                string test_v;
                ALWAYS_ASSERT(tbl->get(txn, Encode(k), test_v));
                
                test::value temp; 
                const test::value *v = Decode(test_v, temp);
                

                tids[n - 1] = v->t_v_tid;

                //fprintf(stderr, "txn %lx get %d  val %lx\n", 
                  //          db->get_tid(txn), n, v->t_v_tid);

                if(type == WRITE || type == PART_WRITE) {
                  test::value v_new(*v);
                  v_new.t_v_count++;
                  v_new.t_v_tid = db->get_tid(txn);
                  tbl->put(txn, Encode(str(), k), Encode(obj_v, v_new));

                  //fprintf(stderr, "txn %lx put %d  val %lx\n", 
                    //        db->get_tid(txn), n, v_new.t_v_tid);

                }
              }

              //bool rp = db->one_op_end(txn);

              bool rp = db->mul_ops_end(txn);

              if(!rp)
                goto retry;

            } catch(piece_abort_exception &ex) {
              goto retry;
            }

          }
  
        res = db->commit_txn(txn);

        if(res) {

          ALWAYS_ASSERT(check(tids, type));
          result++;
        }
        else {
          ALWAYS_ASSERT(false);
          failed++;
        }

      } catch (abstract_db::abstract_abort_exception &ex) {
        ALWAYS_ASSERT(false);
        db->abort_txn(txn);
      }

    return txn_result(res, 0);
  }

  static txn_result
  TxnTransitive(bench_worker *w)
  {
    return static_cast<test_transitive_worker *>(w)->txn_transitive();
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
      w.push_back(workload_desc("Bad Count",  double(g_txn_workload_mix[0])/100.0, TxnTransitive));
    
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

class testtable_transative_loader : public bench_loader {

public:
  testtable_transative_loader(unsigned long seed,
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

        for (size_t i = 1; i <= TESTRecordSize; i++) {
          const test::key k(i);
          const test::value v(0, 0);
          tbl->insert(txn, Encode(k), Encode(obj_buf, v));
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

 #if 1   
    void * const txn2 = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);

    try {
    
      for (size_t i = 1; i <= TESTRecordSize; i++) {

        const test::key k(i);
        string test_v;
        ALWAYS_ASSERT(tbl->get(txn2, Encode(k), test_v));
         test::value temp; 
        const test::value *v = Decode(test_v, temp);
        ALWAYS_ASSERT(v->t_v_count == 0);
      }

      bool res = db->commit_txn(txn2);
      ALWAYS_ASSERT(res);

    } catch (abstract_db::abstract_abort_exception &ex) {
      ALWAYS_ASSERT(false);
      db->abort_txn(txn2);
    }
#endif
    printf("Load Done\n");
  }

};





class testtable_transative_checker : public bench_checker {

public:
  testtable_transative_checker(abstract_db *db,
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
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);

    try {

      size_t i = 1;
      size_t len = 0;

      for(uint32_t u = 1; u <= 2; u++) {

        last_tid = 0;

        if(u == 1) {
          i = 1;
          len = TESTKeySizePerStep * TESTSTEP_PART;
        } else {
          i = TESTKeySizePerStep * TESTSTEP_PART + 1;
          len = TESTKeySizePerStep * TESTSTEP;
        }

        for (; i <= len; i++) {

          const test::key k(i);
          string test_v;
          ALWAYS_ASSERT(tbl->get(txn, Encode(k), test_v));
          test::value temp; 
          const test::value *v = Decode(test_v, temp);

          if(last_tid == 0)
            last_tid = v->t_v_tid;

          fprintf(stderr, "[transitive]: last writer %lx last_tid  %lx\n", v->t_v_tid, last_tid);

          ALWAYS_ASSERT(last_tid == v->t_v_tid);

        }
      }
      bool res = db->commit_txn(txn);
      ALWAYS_ASSERT(res);

    } catch (abstract_db::abstract_abort_exception &ex) {
      ALWAYS_ASSERT(false);
      db->abort_txn(txn);
    }
  }
};



class micro_bench_transitive_runner : public bench_runner {
public:
  micro_bench_transitive_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["TESTTABLE"] = db->open_index("TESTTABLE", TESTRecordSize);
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new testtable_transative_loader(0, db, open_tables));
   
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
        new test_transitive_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }



  virtual std::vector<bench_checker*> make_checkers()
  {
    std::vector<bench_checker*>* bw = new std::vector<bench_checker*>();
    bw->push_back(new testtable_transative_checker(db, open_tables));

    return *bw;
  }

};

void c_graph_test()
{

  cgraph = new conflict_graph(2);
  
  cgraph->init_txn(WRITE, 6);
  cgraph->init_txn(READ, 6);
    
  cgraph->set_conflict(WRITE, 3, READ, 2);
  cgraph->set_conflict(WRITE, 5, READ, 4);
  


  for(int i = 1; i <= 6; i++) {

    auto range1 = cgraph->get_conflict_region(WRITE, i, READ);
    fprintf(stderr, "[WRITE]-ops[%d] conflict range %d -- %d\n", 
        i, range1.first, range1.second);

    auto range2 = cgraph->get_conflict_region(READ, i, WRITE);
    fprintf(stderr, "[READ]-ops[%d] conflict range %d -- %d\n", 
        i, range2.first, range2.second);
  }

}

void
micro_do_trasitive_test(abstract_db *db, int argc, char **argv)
{

  printf("[transitive] Hello World \n");
  
  cgraph = new conflict_graph(TXTYPES);
  
  cgraph->init_txn(WRITE, TESTSTEP);
  cgraph->init_txn(READ, TESTSTEP);
  cgraph->init_txn(PART_WRITE, TESTSTEP_PART);
  cgraph->init_txn(PART_READ, TESTSTEP_PART);

  for(uint32_t i = 1; i <= TESTSTEP; i++) {
    cgraph->set_conflict(WRITE, i, READ, i);
  }

  for(uint32_t i = 1; i <= TESTSTEP_PART; i++) {
    
    cgraph->set_conflict(PART_WRITE, i, PART_READ, i);
  
    cgraph->set_conflict(READ, i, PART_WRITE, i);
    cgraph->set_conflict(READ, i, PART_READ, i);

    cgraph->set_conflict(WRITE, i, PART_WRITE, i);
    cgraph->set_conflict(WRITE, i, PART_READ, i);
  }


  micro_bench_transitive_runner r(db);

  r.run();
}
