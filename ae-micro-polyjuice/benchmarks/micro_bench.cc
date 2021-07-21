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
#include "micro_bench.h"
#include "ndb_wrapper.h"
#include "ndb_wrapper_impl.h"

using namespace std;
using namespace util;


#define PINCPU
#define MAXCPU 64
#define GUASSIAN_DIST

#define TEST_TABLE_LIST(x) \
  x(test) 


static const size_t TXTTPES = 10;



static const int64_t records_per_table = 1024*1024*10;


static bool profile = false;
static size_t txn_length = 10;
static int user_abort_rate = 0;
static uint64_t access_range = 10;


static float read_rate = 0;
static int read_first = 0;
static int piece_access_recs = 1;
// static int uniform_access = 0;

static unsigned g_txn_workload_mix[] = {100};

static double g_zipf_theta = 1;

static conflict_graph* cgraph = nullptr;

enum  MICRO_TYPES
{
  MICROBENCH_1 = 1,
  MICROBENCH_2 = 2,
  MICROBENCH_3 = 3,
  MICROBENCH_4 = 4,
  MICROBENCH_5 = 5,
  MICROBENCH_6 = 6,
  MICROBENCH_7 = 7,
  MICROBENCH_8 = 8,
  MICROBENCH_9 = 9,
  MICROBENCH_10 = 10,
};

#define PROFILE



struct micro_profile {

  uint64_t gettime = 0;
  uint64_t puttime = 0;
  uint64_t piecestarttime = 0;
  uint64_t piececommittime = 0;
  uint64_t txnstarttime = 0;
  uint64_t txncommittime = 0;
  uint64_t succ = 0;

  micro_profile()
  {
    gettime = 0;
    puttime = 0;
    piecestarttime = 0;
    piececommittime = 0;
    txnstarttime = 0;
    txncommittime = 0;
    succ = 0;
  }

  micro_profile& operator+=(const micro_profile& p)
  {                           
    gettime += p.gettime;
    puttime += p.puttime;
    piecestarttime += p.piecestarttime;
    piececommittime += p.piececommittime;
    txnstarttime += p.txnstarttime;
    txncommittime += p.txncommittime;
    succ += p.succ;

    return *this; 
  }

  void print_avg()
  {
    fprintf(stderr, "SUCC[%ld] BEG[TXN]: %ld END[TXN]: %ld BEG[P]: %ld END[P]: %ld GET %ld PUT %ld\n", 
      succ, txnstarttime/succ, txncommittime/succ, 
      piecestarttime/succ, piececommittime/succ, 
      gettime/succ, puttime/succ);
  }

} CACHE_ALIGNED;

static micro_profile pdata[MAXCPU];
static ic3_profile internal_pdata[MAXCPU];

static void print_profile() 
{
  if(!profile)
    return;

  micro_profile avg_prof;

  for(int i = 0; i < MAXCPU; i++) {
    avg_prof += pdata[i];
  }

  avg_prof.print_avg();

  fprintf(stderr, "========================Internal Profiling=========================\n");

  ic3_profile avg_iprof;
  for(int i = 0; i < MAXCPU; i++) {

    for(int j = 0; j < MAX_PROFILE_LEVEL; j++)
    {
      avg_iprof.data[j] += internal_pdata[i].data[j];
    }

    avg_iprof.count += internal_pdata[i].count;
  }

  for(int i = 0; i < MAX_PROFILE_LEVEL; i++)
  {
    if(avg_iprof.data[i] > 0)
      fprintf(stderr, "Level[%d] avg %ld\n", i, avg_iprof.data[i]/avg_iprof.count);
  }

}



static inline ALWAYS_INLINE int
CheckBetweenInclusive(int v, int lower, int upper)
{
  INVARIANT(v >= lower);
  INVARIANT(v <= upper);
  return v;
}

static inline ALWAYS_INLINE int
RandomNumber(fast_random &r, int min, int max)
{
  return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
}

static inline ALWAYS_INLINE int
NonUniformRandom(fast_random &r, int A, int C, int min, int max)
{
  return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
}


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

#define TXN_MICRO_LIST(x) \
  x(0) \
  x(2) \
  x(4) \
  x(6)

  uint32_t acc_per_txn = 8;

#define TXN_MICRO_PIECE_RETRY(a) \
  case a: \
    { \
      goto piece_retry_##a; \
      break; \
    }


int last_insert_key[5] = {0};

class micro_worker : public bench_worker {
public:
  micro_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("TESTTABLE")),
      distribution(2.5),
      computation_n(0),
      pidx(0)
  {
    computation_n = RandomNumber(r, 0, 1000000);

    // initialization
    c = 0;
    alpha = g_zipf_theta;
    n = access_range - 1;

    // Compute normalization constant on first call only
    for (int i=1; i<=n; i++)
    c = c + (1.0 / pow((double) i, alpha));
    c = 1.0 / c;

    sum_probs = malloc((n+1)*sizeof(*sum_probs));
    sum_probs[0] = 0;
    for (int i=1; i<=n; i++) {
      sum_probs[i] = sum_probs[i-1] + c / pow((double) i, alpha);
    }
  }

  double alpha;
  int n;
  double c;             // Normalization constant
  double *sum_probs;    // Pre-calculated sum of probabilities

  int ZipfInt(fast_random &r)
  {
    double z;                     // Uniform random number (0 < z < 1)
    int zipf_value;               // Computed exponential value to be returned
    int i;                        // Loop counter
    int low, high, mid;           // Binary-search bounds

    // Pull a uniform random number (0 < z < 1)
    do
    {
        z = r.next_uniform();
    }
    while ((z == 0) || (z == 1));

    // Map z to the value
    low = 1, high = n, mid;
    do {
        mid = floor((low+high)/2);
        if (sum_probs[mid] >= z && sum_probs[mid-1] < z) {
            zipf_value = mid;
            break;
        } else if (sum_probs[mid] >= z) {
            high = mid-1;
        } else {
            low = mid+1;
        }
    } while (low <= high);

    // Assert that zipf_value is between 1 and N
    assert((zipf_value >=1) && (zipf_value <= n));

    return(zipf_value);
  }

  util::fast_random &getR(){
    return r;
  }

  txn_result
  txn_micro()
  {

    bool res = false;

    size_t read_records = read_rate * txn_length;
    size_t write_records = txn_length - read_records;

    bool user_abort = (user_abort_rate > 0);
    uint64_t abort_op = txn_length + 1;

    if(user_abort) {

      uint64_t abort_n = 100 / user_abort_rate;
      computation_n++;

      if(computation_n % abort_n == 0) {
        abort_op = RandomNumber(r, 0, txn_length - 1);
      } 
    }

    uint64_t start_txn_beg = 0;
    if(profile)
      start_txn_beg = rdtsc();

    scoped_str_arena s_arena(arena);
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);
    db->init_txn(txn, cgraph, MICROBENCH_1);

    if(profile)
      pdata[pidx].txnstarttime += rdtsc() - start_txn_beg;

    try {

          
          
          int base = generate_key(0);

          for(size_t i = 0; i < txn_length; i++) {

            retry:
            try {
              
              int n;

              if( i == 0) {
                n = base;
              } else {
                n =   generate_key(i);
              }

              // fprintf(stderr, "[%d: %lu] update record %d \n", worker_id, i, n);
              // int n = base + i * records_per_table;

              uint64_t start_piece_beg = 0;
              if(profile)
                start_piece_beg = rdtsc();

              db->one_op_begin(txn);
              // db->mul_ops_end_ops_begin(txn);

              if(profile)
                pdata[pidx].piecestarttime += rdtsc() - start_piece_beg ;
    


              // printf("Thread{%d} n %lu min %lu max %lu\n", worker_id, n, i*records_per_table, i * records_per_table + access_range - 1);
              // n += i * records_per_table;

              const test::key k(n);

              uint64_t get_beg = 0; 
              if(profile)
                get_beg = rdtsc();

              if(profile) {
                tbl->get_profile(txn, Encode(obj_key0, k), obj_v, std::string::npos, &internal_pdata[pidx]);
                internal_pdata[pidx].count++;
              } else {
                tbl->get(txn, Encode(obj_key0, k), obj_v);
              }  

              if(profile)
                pdata[pidx].gettime += rdtsc() - get_beg ;

              if(abort_op == i) {
                db->abort_txn(txn);
                return txn_result(false, 0);
              }

              test::value temp; 
              const test::value *v = Decode(obj_v, temp);

              if(read_first && read_records > 0)
                read_records--;
              
              if((read_first && read_records == 0) ||
                ((!read_first) && write_records > 0)) {


              uint64_t put_beg = 0;
              if(profile)
                put_beg = rdtsc();

              test::value v_new(*v);
              v_new.t_v_count++;   
              ALWAYS_ASSERT(v_new.t_v_count > 0);
              tbl->put(txn, Encode(str(), k), Encode(obj_v, v_new));

              if(profile) 
                pdata[pidx].puttime += rdtsc() - put_beg ;


                if(!read_first)
                  write_records--;
              }


              uint64_t end_piece_beg = 0;
              if(profile)
                end_piece_beg = rdtsc();

              // bool rp = db->mul_ops_end(txn);
              bool rp = db->one_op_end(txn);

              if(profile) 
                pdata[pidx].piececommittime += rdtsc() - end_piece_beg ;


              if(!rp)
                goto retry;

            } catch(piece_abort_exception &ex) {
              goto retry;
            }
          }


        uint64_t end_txn_beg = 0;
        if(profile)
          end_txn_beg = rdtsc();

        //fprintf(stderr, "%ld, %ld, %ld\n", oldv, newv, coreid::core_id());
        res = db->commit_txn(txn);
        //ALWAYS_ASSERT(res);

        if(profile){
          pdata[pidx].txncommittime += rdtsc() - end_txn_beg ;
          pdata[pidx].succ++;
        }


      } catch (abstract_db::abstract_abort_exception &ex) {
  
        db->abort_txn(txn);
      }

    return txn_result(res, 0);
  }

  txn_result
  txn_mul_micro(MICRO_TYPES type)
  {
    bool user_abort = (user_abort_rate > 0);
    uint64_t abort_op = txn_length + 1;

    if(user_abort) {

      uint64_t abort_n = 100 / user_abort_rate;

      computation_n = r.next();

      if(computation_n % abort_n == 0) {
        abort_op = RandomNumber(r, 0, txn_length - 1);
      }
    }

    bool res = false;

    scoped_str_arena s_arena(arena);
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);
    db->init_txn(txn, cgraph, type, policy, pg);
    db->set_failed_records(txn, failed_records);

    try {
          std::pair<bool, uint32_t> expose_ret;
          int high_contention_pieces = 2;


          {
piece_retry_0:
            int base = generate_key(0);
            const test::key k(base);

            ALWAYS_ASSERT(tbl->get(txn, Encode(obj_key0, k), obj_v, std::string::npos, 0 + (type - 1) * acc_per_txn /*access_id*/));

            expose_ret = db->expose_uncommitted(txn, 0 + (type - 1) * acc_per_txn + ACCESSES/*access_id*/);
            if (!expose_ret.first) {
              switch (expose_ret.second - (type - 1) * acc_per_txn) {
                TXN_MICRO_LIST(TXN_MICRO_PIECE_RETRY)
                default: ALWAYS_ASSERT(false);
              }
            }

piece_retry_1:  
            test::value temp; 
            const test::value *v = Decode(obj_v, temp);
      
            test::value v_new(*v);
            v_new.t_v_count++;   
            ALWAYS_ASSERT(v_new.t_v_count > 0);

            tbl->put(txn, Encode(str(), k), Encode(obj_v, v_new), 1 + (type - 1) * acc_per_txn /*access_id*/);

            expose_ret = db->expose_uncommitted(txn, 1 + (type - 1) * acc_per_txn + ACCESSES/*access_id*/);
            if (!expose_ret.first) {
              switch (expose_ret.second - (type - 1) * acc_per_txn) {
                TXN_MICRO_LIST(TXN_MICRO_PIECE_RETRY)
                default: ALWAYS_ASSERT(false);
              }
            }
          }
          
piece_retry_2:
          {
            int keys[piece_access_recs - 1] = {0};
            for(int j = 1; j < piece_access_recs; j++) {

                keys[j - 1] = RandomNumber(r, 0 * records_per_table + access_range, (0 + 1) * records_per_table - 1);

            }
            std::sort(keys, keys + piece_access_recs - 1);

            for(int j = 1; j < piece_access_recs; j++) {
              const test::key k(keys[j - 1]);
              ALWAYS_ASSERT(tbl->get(txn, Encode(obj_key0, k), obj_v, std::string::npos, 2 + (type - 1) * acc_per_txn /*access_id*/));
              test::value temp; 
              const test::value *v = Decode(obj_v, temp);
      
              test::value v_new(*v);
              v_new.t_v_count++;   
              ALWAYS_ASSERT(v_new.t_v_count > 0);

              tbl->put(txn, Encode(str(), k), Encode(obj_v, v_new), 3 + (type - 1) * acc_per_txn /*access_id*/);
            }
            expose_ret = db->expose_uncommitted(txn, 3 + (type - 1) * acc_per_txn + ACCESSES/*access_id*/);
            if (!expose_ret.first) {
              switch (expose_ret.second - (type - 1) * acc_per_txn) {
              TXN_MICRO_LIST(TXN_MICRO_PIECE_RETRY)
                  default: ALWAYS_ASSERT(false);
              }
            }
          }
piece_retry_4:
          {
            int keys[piece_access_recs * 6] = {0};
            for(int j = 0; j < piece_access_recs * 6; j++) {

                keys[j] = RandomNumber(r, 1 * records_per_table + access_range, (1 + 1) * records_per_table - 1);

            }
            std::sort(keys, keys + piece_access_recs * 6);

            for(int j = 0; j < piece_access_recs * 6; j++) {
              const test::key k(keys[j]);
              ALWAYS_ASSERT(tbl->get(txn, Encode(obj_key0, k), obj_v, std::string::npos, 4 + (type - 1) * acc_per_txn /*access_id*/));
              test::value temp; 
              const test::value *v = Decode(obj_v, temp);
      
              test::value v_new(*v);
              v_new.t_v_count++;   
              ALWAYS_ASSERT(v_new.t_v_count > 0);

              tbl->put(txn, Encode(str(), k), Encode(obj_v, v_new), 5 + (type - 1) * acc_per_txn /*access_id*/);
            }
            expose_ret = db->expose_uncommitted(txn, 5 + (type - 1) * acc_per_txn + ACCESSES/*access_id*/);
            if (!expose_ret.first) {
              switch (expose_ret.second - (type - 1) * acc_per_txn) {
              TXN_MICRO_LIST(TXN_MICRO_PIECE_RETRY)
                  default: ALWAYS_ASSERT(false);
              }
            }
          }

          {
piece_retry_6:
            int final_acc_key = RandomNumber(r, (2 + type - 1) * records_per_table, (2 + type) * records_per_table - 1);
            const test::key k(final_acc_key);

            ALWAYS_ASSERT(tbl->get(txn, Encode(obj_key0, k), obj_v, std::string::npos, 6 + (type - 1) * acc_per_txn /*access_id*/));

            expose_ret = db->expose_uncommitted(txn, 6 + (type - 1) * acc_per_txn + ACCESSES/*access_id*/);
            if (!expose_ret.first) {
              switch (expose_ret.second - (type - 1) * acc_per_txn) {
              TXN_MICRO_LIST(TXN_MICRO_PIECE_RETRY)
                  default: ALWAYS_ASSERT(false);
              }
            }

piece_retry_7:
            test::value temp; 
            const test::value *v = Decode(obj_v, temp);

            test::value v_new(*v);
            v_new.t_v_count++;   
            ALWAYS_ASSERT(v_new.t_v_count > 0);

            tbl->put(txn, Encode(str(), k), Encode(obj_v, v_new), 7 + (type - 1) * acc_per_txn /*access_id*/);

            expose_ret = db->expose_uncommitted(txn, 7 + (type - 1) * acc_per_txn + ACCESSES/*access_id*/);
            if (!expose_ret.first) {
              switch (expose_ret.second - (type - 1) * acc_per_txn) {
              TXN_MICRO_LIST(TXN_MICRO_PIECE_RETRY)
                  default: ALWAYS_ASSERT(false);
              }
            }
          }

        //fprintf(stderr, "%ld, %ld, %ld\n", oldv, newv, coreid::core_id());
        res = db->commit_txn(txn);
        set_failed_records(db->get_failed_records(txn));
        finished_txn_contention = db->get_txn_contention(txn);
        // if(res){
        //   for(size_t i = 0; i < 5; i++) {
        //     last_insert_key[i] = (keys_contention[i] - (i + 10) * records_per_table) % records_per_table;
        //     // printf("%d, %d, %d\n", i, last_insert_key[i], keys_contention[i]);
        //   }
        // }
        //ALWAYS_ASSERT(res);
      } catch(transaction_abort_exception &ex) {
        db->abort_txn(txn);
      } catch (abstract_db::abstract_abort_exception &ex) {
        db->abort_txn(txn);
      }

    return txn_result(res, type);
  }

  static txn_result
  TxnMicro_1(bench_worker *w)
  {
    return static_cast<micro_worker *>(w)->txn_mul_micro(MICROBENCH_1);
  }

  static txn_result
  TxnMicro_2(bench_worker *w)
  {
    return static_cast<micro_worker *>(w)->txn_mul_micro(MICROBENCH_2);
  }

  static txn_result
  TxnMicro_3(bench_worker *w)
  {
    return static_cast<micro_worker *>(w)->txn_mul_micro(MICROBENCH_3);
  }

  static txn_result
  TxnMicro_4(bench_worker *w)
  {
    return static_cast<micro_worker *>(w)->txn_mul_micro(MICROBENCH_4);
  }

  static txn_result
  TxnMicro_5(bench_worker *w)
  {
    return static_cast<micro_worker *>(w)->txn_mul_micro(MICROBENCH_5);
  }

  static txn_result
  TxnMicro_6(bench_worker *w)
  {
    return static_cast<micro_worker *>(w)->txn_mul_micro(MICROBENCH_6);
  }

  static txn_result
  TxnMicro_7(bench_worker *w)
  {
    return static_cast<micro_worker *>(w)->txn_mul_micro(MICROBENCH_7);
  }

  static txn_result
  TxnMicro_8(bench_worker *w)
  {
    return static_cast<micro_worker *>(w)->txn_mul_micro(MICROBENCH_8);
  }

  static txn_result
  TxnMicro_9(bench_worker *w)
  {
    return static_cast<micro_worker *>(w)->txn_mul_micro(MICROBENCH_9);
  }

  static txn_result
  TxnMicro_10(bench_worker *w)
  {
    return static_cast<micro_worker *>(w)->txn_mul_micro(MICROBENCH_10);
  }

  virtual workload_desc_vec
  get_workload() const
  {
  
    workload_desc_vec w;
    unsigned m = 0;
    for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
      m += g_txn_workload_mix[i];
    ALWAYS_ASSERT(m == 100);
    // if (g_txn_workload_mix[0])
    //   w.push_back(workload_desc("Micro bench",  double(g_txn_workload_mix[0])/100.0, TxnMultipleMicro));
    
    // all 10 txn type has same possibility
    if (g_txn_workload_mix[0]) {
      w.push_back(workload_desc("Micro_bench_1",  double(0.1), TxnMicro_1));
      w.push_back(workload_desc("Micro_bench_2",  double(0.1), TxnMicro_2));
      w.push_back(workload_desc("Micro_bench_3",  double(0.1), TxnMicro_3));
      w.push_back(workload_desc("Micro_bench_4",  double(0.1), TxnMicro_4));
      w.push_back(workload_desc("Micro_bench_5",  double(0.1), TxnMicro_5));
      w.push_back(workload_desc("Micro_bench_6",  double(0.1), TxnMicro_6));
      w.push_back(workload_desc("Micro_bench_7",  double(0.1), TxnMicro_7));
      w.push_back(workload_desc("Micro_bench_8",  double(0.1), TxnMicro_8));
      w.push_back(workload_desc("Micro_bench_9",  double(0.1), TxnMicro_9));
      w.push_back(workload_desc("Micro_bench_10",  double(0.1), TxnMicro_10));
    }
    
    return w;
  }

protected:

  virtual void
  on_run_setup() OVERRIDE
  {
    bind_thread(worker_id);
    pidx = (worker_id - coreid::num_cpus_online());
  }

  inline ALWAYS_INLINE string &
  str() {
    return *arena.next();
  }

  inline ALWAYS_INLINE int 
  generate_key(unsigned int table_id)
  {

// #ifdef GUASSIAN_DIST
//     int offset = (int)ceil(distribution(generator) + worker_id) % access_range;//RandomNumber(r, 0, access_range - 1);
// #else  
//     int offset = RandomNumber(r, 0, access_range - 1);
// #endif    
    int offset = ZipfInt(getR());
    return offset + table_id * records_per_table;  
  }

private:
  abstract_ordered_index *tbl;

  string obj_key0;
  string obj_key1;
  string obj_v;

  std::default_random_engine generator;
  std::exponential_distribution<float> distribution;

  uint64_t computation_n;
  int pidx;
};

class microbench_loader : public bench_loader{

public:
microbench_loader(unsigned long seed,
                        abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables,
                        int64_t min, int64_t max)
    : bench_loader(seed, db, open_tables), min(min), max(max)
  {}

protected:
  virtual void
  load()
  {

    abstract_ordered_index *tbl = open_tables.at("TESTTABLE");
    string obj_buf;
    int64_t batch_size = 512;
    ALWAYS_ASSERT(records_per_table > batch_size && records_per_table % batch_size == 0);
    ALWAYS_ASSERT(max > 0 && min >= 0);

    scoped_str_arena s_arena(arena);
    
    for(int64_t i = min/batch_size; i < max/batch_size; i++)
    {

      void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO_LOADER);
      try {

        for (int64_t n = i * batch_size; n < (i + 1) * batch_size; n++) {
            // printf("load %ld\n", n);
            const test::key k(n);
            const test::value v(0, "TEST");
            tbl->insert(txn, Encode(k), Encode(obj_buf, v));

        }

        bool res = db->commit_txn(txn);
        ALWAYS_ASSERT(res);

      } catch (abstract_db::abstract_abort_exception &ex) {
        ALWAYS_ASSERT(false);
        db->abort_txn(txn);
      }
    }
    
    // printf("Load Done [%ld] --- [%ld]\n", min, max);
  }

private:
  int64_t min, max;
};





class microbench_checker : public bench_checker {

public:
  microbench_checker(abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
    : bench_checker(db, open_tables)
  {}

protected:
  virtual void
  check()
  {}
};



class micro_bench_runner : public bench_runner {
public:
  micro_bench_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["TESTTABLE"] = db->open_index("TESTTABLE", (12 * records_per_table));
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;

    uint64_t total = 12 * records_per_table;
    if(enable_parallel_loading) {

      const unsigned alignment = coreid::num_cpus_online();
      
      ALWAYS_ASSERT(total % alignment == 0);

      uint64_t range = total/alignment;

      for(size_t i = 0; i < alignment; i++) {

        uint64_t min = range * i;
        uint64_t max = range * (i + 1);

        ret.push_back(new microbench_loader(0, db, open_tables, min, max));
      }
    } else {
        ret.push_back(new microbench_loader(0, db, open_tables, 0, total));
    }
   
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
        new micro_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }



  virtual std::vector<bench_checker*> make_checkers()
  {
    std::vector<bench_checker*>* bw = new std::vector<bench_checker*>();
    bw->push_back(new microbench_checker(db, open_tables));

    return *bw;
  }

};

void
microbench_do_test(abstract_db *db, int argc, char **argv)
{

  // printf("Run Micro Benchmark %d %s\n", argc, *argv);

  optind = 1;

  while (1) {
    static struct option long_options[] =
    {
      {"access-range" , required_argument , 0, 'a'},
      {"txn-length"    , required_argument , 0, 't'},
      {"user-initial-abort"    , required_argument , 0, 'u'},
      {"piece-access-recs"    , required_argument , 0, 'p'},
      {"zipf-theta"           , required_argument , 0, 'z'} ,
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "a:z:t:p:u", long_options, &option_index);
    if (c == -1)
      break;

        /* getopt_long stores the option index here. */

      switch (c)
      {
        case 0:
        if (long_options[option_index].flag != 0)
          break;
        abort();
        break;

        case 'a':
        access_range = atoi(optarg);
        // cout<<"access range "<< access_range <<endl;
        ALWAYS_ASSERT(access_range > 0);
        break;

        case 't':
        txn_length  = atoi(optarg);    
        // cout<<"txn length "<< txn_length<<endl;
        ALWAYS_ASSERT(txn_length > 0);
        break;

        case 'p':
        piece_access_recs  = atoi(optarg);    
        // cout<<"piece access recs "<< piece_access_recs<<endl;
        ALWAYS_ASSERT(piece_access_recs > 0);
        break;

        case 'u':
        user_abort_rate  = atoi(optarg);    
        // cout<<"user abort rate "<< user_abort_rate<< "%%" <<endl;
        ALWAYS_ASSERT(user_abort_rate >= 0);
        break;

        case 'z':
        g_zipf_theta = atof(optarg);
        // cout<<"g_zipf_theta "<< g_zipf_theta <<endl;
        ALWAYS_ASSERT(g_zipf_theta >= 0);
        break;

        default:
          fprintf(stderr, "Wrong Arg %d\n", c);
          exit(1);
      }
     
  }

  // fprintf(stderr, "txn length %lu access_range %lu piece_access_recs %d\n", 
    // txn_length,  access_range, piece_access_recs);

  cgraph = new conflict_graph(TXTTPES);
  cgraph->init_txn(MICROBENCH_1, 10);
  cgraph->init_txn(MICROBENCH_2, 10);
  cgraph->init_txn(MICROBENCH_3, 10);
  cgraph->init_txn(MICROBENCH_4, 10);
  cgraph->init_txn(MICROBENCH_5, 10);
  cgraph->init_txn(MICROBENCH_6, 10);
  cgraph->init_txn(MICROBENCH_7, 10);
  cgraph->init_txn(MICROBENCH_8, 10);
  cgraph->init_txn(MICROBENCH_9, 10);
  cgraph->init_txn(MICROBENCH_10, 10);

  for(uint8_t i = 1; i <= 9; ++i) {
    for(uint8_t j = i + 1; j <= 10; ++j) {
      for(uint32_t op = 1; op <= 10; ++op) {
        cgraph->set_conflict(i, op, j, op);
      }
    }
  }
  micro_bench_runner r(db);

  if (kid_end > 0)
    r.training_run(policies_to_eval);
  else
    r.run();

  print_profile();
}
