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
#include "micro_pieces.h"

#include "ndb_wrapper.h"
 #include "ndb_wrapper_impl.h"

using namespace std;
using namespace util;

#define TEST_TABLE_LIST(x) \
  x(test_piece)

static const size_t NRECORDS = 10000;

static unsigned g_txn_workload_mix[] = {100};

#define PIECECOUNT 10

class test_piece_worker;

namespace test_piece {

    struct param_t {

      test_piece_worker* worker;
      abstract_ordered_index* tbl;
      void* txn;
      uint64_t pid;
    };

    struct piece_exec {
      param_t param;
      void* res;

      void *(*func)(param_t);
    };

    void* piece_func(param_t p);

};

class test_piece_worker : public bench_worker {

public:
  test_piece_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("TESTTABLE")),
      computation_n(0)
  {}


  txn_result 
  txn_with_pieces()
  {

    string obj_buf;
    bool res = false;
    scoped_str_arena s_arena(arena);
    void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);

    // fprintf(stderr, "Tiger 11111111111111\n");

    for(int i = 0; i < PIECECOUNT; i++) {

      pieces[i].param.worker = this;
      pieces[i].param.tbl = tbl;
      pieces[i].param.txn = txn;
      pieces[i].param.pid = i;

      pieces[i].func(pieces[i].param);
    }

    // fprintf(stderr, "Tiger 22222222222222222\n");

    res = db->commit_txn(txn);

    // fprintf(stderr, "Tiger 3333333333333333\n");

    ALWAYS_ASSERT(res);
    return txn_result(res, 0);
  }


  txn_result
  txn_without_pieces()
  {

    string obj_buf;
    bool res = false;
    scoped_str_arena s_arena(arena);
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);

    //piece_abort_exceptiontf("READ WRITE TXN\n");  
    try {

          
          for (size_t s = 0; s < PIECECOUNT; s++) {

              const piece_test::key k(s);
              string test_v;

              ALWAYS_ASSERT(tbl->get_for_read(txn, Encode(k), test_v));
              
              // piece_test::value temp; 
              // const piece_test::value *v = Decode(test_v, temp);
              
              // piece_test::value v_new(*v);
              // v_new.t_v_count++;
              // ALWAYS_ASSERT(v_new.t_v_count > 0);
            
              // tbl->put(txn, Encode(obj_key0, k), Encode(obj_v, v_new));
              
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
  TxnWithPieces(bench_worker *w)
  {
    return static_cast<test_piece_worker *>(w)->txn_with_pieces();
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
      w.push_back(workload_desc("Txn With Pieces",  double(g_txn_workload_mix[0])/100.0, TxnWithPieces));
    
    return w;
  }

public:

  virtual void
  on_run_setup() OVERRIDE {

    for(int i = 0; i < PIECECOUNT; i++) {
      pieces[i].func = test_piece::piece_func;
    }
  }

  inline ALWAYS_INLINE string &
  str() {
    return *arena.next();
  }

public:
  abstract_ordered_index *tbl;

  string obj_key0;
  string obj_key1;
  string obj_v;

  uint64_t computation_n;

  test_piece::piece_exec pieces[PIECECOUNT];

};


void* test_piece::piece_func(param_t p) {

      void* txn = p.txn;

      abstract_ordered_index* tbl = p.tbl;
      test_piece_worker* worker = p.worker;

      const piece_test::key k(p.pid);

      string test_v;

      ALWAYS_ASSERT(tbl->get_for_read(txn, Encode(k), test_v));
              
      // piece_test::value temp; 
      // const piece_test::value *v = Decode(test_v, temp);
              
      // piece_test::value v_new(*v);
      // v_new.t_v_count++;
      // ALWAYS_ASSERT(v_new.t_v_count > 0);
      

      // const std::string k0 = Encode(worker->obj_key0, k);
      // const std::string v0 = Encode(worker->obj_v, v_new);

      // tbl->put(txn, k0, v0);

      return nullptr;
}

class testpieces_loader: public bench_loader {

public:
  testpieces_loader(unsigned long seed,
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
        const piece_test::key k(i);
        const piece_test::value v(0);
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


class micro_pieces_runner : public bench_runner {
public:
  micro_pieces_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["TESTTABLE"] = db->open_index("TESTTABLE", NRECORDS);
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new testpieces_loader(0, db, open_tables));
   
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
        new test_piece_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }

};

void
micro_pieces_do_test(abstract_db *db, int argc, char **argv)
{
  printf("Micro Pieces Test\n");

  micro_pieces_runner r(db);

  r.run();
}
