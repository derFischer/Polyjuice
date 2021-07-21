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
#include "micro.h"
#include "ndb_wrapper.h"
using namespace std;
using namespace util;

#define TEST_TABLE_LIST(x) \
  x(test) 


class test_worker : public micro_worker {
public:
  test_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : micro_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("TESTTABLE")){}


  virtual txn_result micro_txn()
  {
    string obj_buf;
    bool res = false;
    scoped_str_arena s_arena(arena);
    ndb_wrapper_default* ndb = (ndb_wrapper_default*)db;
    txn_conflict_default *txn = ndb->new_conflict_txn(txn_flags, arena);      
    
    
    int check = 0;
    int rev0 = 0;
    int rev = 0;

    for (size_t i = TESTKeySize; i >= 1; i--) {
      
      const test::key k(1);

      test::value* v = (test::value*) txn->get(tbl->get_tree(), Encode(str(), k));

      rev0 = v->t_v_count;
      test::value temp(*v);
      temp.t_v_count++;

      //fprintf(stderr, "length %d\n", str->length());
      txn->put(tbl->get_tree(), Encode(str(), k), (char *)&temp);

      v = (test::value*) txn->get(tbl->get_tree(), Encode(str(), k));
      rev = v->t_v_count;

      //ALWAYS_ASSERT(v->t_v_count == check);
    } 

    res = txn->commit();


    if(res) {
        result++;
    }
    else {
        failed++;
    return txn_result(res, 0);
  }

  
protected:

private:
  abstract_ordered_index *tbl;
  uint64_t computation_n;
};



