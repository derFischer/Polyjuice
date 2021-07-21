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
#include "micro_bench.h"
#include "ndb_wrapper.h"
#include "ndb_wrapper_impl.h"

using namespace std;
using namespace util;

#define PINCPU

#define TEST_TABLE_LIST(x) \
  x(test) 


static const size_t TXTTPES = 1;

static const int64_t records_per_table = 1024*1024;

static bool profile = false;
static size_t txn_length = 10;
static uint64_t access_range = 10;
static int user_abort_rate = 0;

static float read_rate = 0;
static int read_first = 0;
static int piece_access_recs = 1;

static unsigned g_txn_workload_mix[] = {100};

#define MAXCPU 64
#define GUASSIAN_DIST

enum  MICRO_TYPES
{
  MICROBENCH = 1,
};



struct app_profile {

  enum level
  {
    LEVEL0 = 0,
    LEVEL1,
    LEVEL2,
    LEVEL3,
    LEVEL4,
    LEVEL5,
    LEVEL6,
    LEVEL7,
    LEVEL8,
  };

#define APP_PROFILE_LEVEL 15

  uint64_t data[APP_PROFILE_LEVEL];
  uint32_t count; 
  uint32_t padding; 

  app_profile()
  {
    for(int i = 0; i < APP_PROFILE_LEVEL; i++)
      data[i] = 0;
    count = 0;
  }

  
} CACHE_ALIGNED;


static app_profile pdata[MAXCPU];
static ic3_profile internal_pdata[MAXCPU];

static void print_profile() 
{
  fprintf(stderr, "========================APP Profiling=========================\n");

  app_profile avg_aprof;
  for(int i = 0; i < MAXCPU; i++) {

    for(int j = 0; j < APP_PROFILE_LEVEL; j++)
    {
      avg_aprof.data[j] += pdata[i].data[j];
    }

    avg_aprof.count += pdata[i].count;
  }

  for(int i = 0; i < APP_PROFILE_LEVEL; i++)
  {
    if(avg_aprof.data[i] > 0)
      fprintf(stderr, "Level[%d] avg %ld\n", i, avg_aprof.data[i]/avg_aprof.count);
  }

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
      computation_n(0)
  {}

  txn_result
  txn_micro()
  {

    string obj_buf;
    bool res = false;

    size_t read_records = read_rate * txn_length;
    size_t write_records = txn_length - read_records;

    scoped_str_arena s_arena(arena);
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_MICRO);

    try {

          int base = generate_key(0);


        for(size_t i = 0; i < txn_length; i++) {

            int n;

            if( i == 0) {
              n = base;
            } else {
              n =   generate_key(i);
            }


            //printf("Thread{%d} min %d max %d\n", worker_id,  min, max);
            // n += i * records_per_table;
            bool is_write = true;

            const test::key k(n);

            if((read_first && read_records > 0)||
                ((!read_first) && write_records == 0)) {
              
              ALWAYS_ASSERT(tbl->get_for_read(txn, Encode(obj_key0, k), obj_v));

              read_records--;
              is_write = false;

            } else {

              ALWAYS_ASSERT(tbl->get(txn, Encode(obj_key0, k), obj_v));

              write_records--;
              is_write = true;

            }

            test::value temp; 
            const test::value *v = Decode(obj_v, temp);
            
            if(is_write) {
              test::value v_new(*v);
              v_new.t_v_count++;   
              ALWAYS_ASSERT(v_new.t_v_count > 0);
              tbl->put(txn, Encode(str(), k), Encode(str(), v_new));
            }

        }

        //fprintf(stderr, "%ld, %ld, %ld\n", oldv, newv, coreid::core_id());
        res = db->commit_txn(txn);
        //ALWAYS_ASSERT(res);

      } catch (abstract_db::abstract_abort_exception &ex) {
  
        db->abort_txn(txn);
      }

    return txn_result(res, 0);
  }

  txn_result
  txn_mul_micro()
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

    try {

          for(size_t i = 0; i < txn_length; i++) {


            if(abort_op == i) {
                db->abort_txn(txn);
                return txn_result(false, 0);
            }

              uint64_t gen_beg = 0;
              if(profile)
                  gen_beg = rdtsc();

              
              int base = generate_key(i);

              std::vector<int> keys;
              keys.push_back(base);

              for(int j = 1; j < piece_access_recs; j++) {

                  keys.push_back(RandomNumber(r, i * records_per_table, (i + 1) * records_per_table - 1));

              }

              uint64_t sort_beg = 0;
              if(profile) {
                  pdata[pidx].data[app_profile::level::LEVEL0] += rdtsc() - gen_beg;
                  sort_beg = rdtsc();
              }

              std::sort(keys.begin(), keys.end());

              if(profile) {
                  pdata[pidx].data[app_profile::level::LEVEL1] += rdtsc() - sort_beg;
              }

              for(int j = 0; j < piece_access_recs; j++) {

                  uint64_t get_beg = 0;
                  if(profile)
                      get_beg = rdtsc();
                  
                  const test::key k(keys[j]);
                  if(profile)
                    ALWAYS_ASSERT(tbl->get_profile(txn, Encode(obj_key0, k), obj_v, std::string::npos, &internal_pdata[pidx]));
                  else
                    ALWAYS_ASSERT(tbl->get(txn, Encode(obj_key0, k), obj_v));

                  uint64_t put_beg = 0;
                  if(profile) {
                      pdata[pidx].data[app_profile::level::LEVEL2] += rdtsc() - get_beg;
                      put_beg = rdtsc();
                  }

                  // printf("W[%d] P[%lu] r[%d] rec[%d]\n", worker_id, i, j, keys[j]);
                
                  test::value temp; 
                  const test::value *v = Decode(obj_v, temp);
      
                  test::value v_new(*v);
                  v_new.t_v_count++;   
                  ALWAYS_ASSERT(v_new.t_v_count > 0);
                
                  tbl->put(txn, Encode(str(), k), Encode(obj_v, v_new));
                  if(profile) {
                      pdata[pidx].data[app_profile::level::LEVEL3] += rdtsc() - put_beg;
                  }
              
              }

          }
        
        uint64_t commit_beg = 0;
        if(profile) {
            commit_beg = rdtsc();
        }

        //fprintf(stderr, "%ld, %ld, %ld\n", oldv, newv, coreid::core_id());
        res = db->commit_txn(txn);
        //ALWAYS_ASSERT(res);

        if(profile) {
            pdata[pidx].data[app_profile::level::LEVEL4] += rdtsc() - commit_beg;
            pdata[pidx].count++;
            internal_pdata[pidx].count++;
        }


      } catch (abstract_db::abstract_abort_exception &ex) {
  
        db->abort_txn(txn);
      }

    return txn_result(res, 0);
  }


  static txn_result
  TxnMicro(bench_worker *w)
  {
    return static_cast<micro_worker *>(w)->txn_mul_micro();
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
    pidx = (worker_id - coreid::num_cpus_online());
  }

  inline ALWAYS_INLINE string &
  str() {
    return *arena.next();
  }

  inline ALWAYS_INLINE int 
  generate_key(unsigned int table_id)
  {

#ifdef GUASSIAN_DIST
    int offset = (int)ceil(distribution(generator) + worker_id) % access_range;//RandomNumber(r, 0, access_range - 1);
#else  
    int offset = RandomNumber(r, 0, access_range - 1);
#endif    
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
    
    printf("Load Done [%ld] --- [%ld]\n", min, max);
  }

private:
  int64_t min, max;
};





class micro_bench_runner : public bench_runner {
public:
  micro_bench_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["TESTTABLE"] = db->open_index("TESTTABLE", (txn_length * records_per_table));
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;

    uint64_t total = txn_length * records_per_table;
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



};

void
microbench_do_test(abstract_db *db, int argc, char **argv)
{

  printf("Run Micro Benchmark %d %s\n", argc, *argv);

  optind = 1;

  while (1) {
    static struct option long_options[] =
    {
      {"access-range" , required_argument , 0, 'a'},
      {"txn-length"    , required_argument , 0, 't'},
      {"user-initial-abort"    , required_argument , 0, 'u'},
      {"piece-access-recs"    , required_argument , 0, 'p'},
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "a:t:p:u", long_options, &option_index);
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
        cout<<"access range "<< access_range <<endl;
        ALWAYS_ASSERT(access_range > 0);
        break;

        case 't':
        txn_length  = atoi(optarg);    
        cout<<"txn length "<< txn_length<<endl;
        ALWAYS_ASSERT(txn_length > 0);
        break;

        case 'p':
        piece_access_recs  = atoi(optarg);    
        cout<<"piece access recs "<< piece_access_recs<<endl;
        ALWAYS_ASSERT(piece_access_recs > 0);
        break;

        case 'u':
        user_abort_rate  = atoi(optarg);    
        cout<<"user abort rate "<< user_abort_rate<< "%%" <<endl;
        ALWAYS_ASSERT(user_abort_rate >= 0);
        break;


        default:
          fprintf(stderr, "Wrong Arg %d\n", c);
          exit(1);
      }
     
  }

  fprintf(stderr, "txn length %lu access_range %lu piece_access_recs %d\n", 
    txn_length,  access_range, piece_access_recs);

  micro_bench_runner r(db);

  r.run();
  if(profile) {
    print_profile();
    fprintf(stderr, "Contention %ld\n", n_contention.load());
  }
}
