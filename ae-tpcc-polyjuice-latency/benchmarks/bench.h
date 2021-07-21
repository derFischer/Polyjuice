#ifndef _NDB_BENCH_H_
#define _NDB_BENCH_H_

#include <stdint.h>

#include <map>
#include <vector>
#include <utility>
#include <string>

#include "abstract_db.h"
#include "../macros.h"
#include "../thread.h"
#include "../util.h"
#include "../spinbarrier.h"
#include "../rcu.h"

extern void ycsb_do_test(abstract_db *db, int argc, char **argv);
extern void tpcc_do_test(abstract_db *db, int argc, char **argv);
extern void queue_do_test(abstract_db *db, int argc, char **argv);
extern void encstress_do_test(abstract_db *db, int argc, char **argv);
extern void tpce_do_test(abstract_db *db, int argc, char **argv);
extern void small_do_test(abstract_db *db, int argc, char **argv);
extern void seats_do_test(abstract_db *db, int argc, char **argv);
//All micro tests
extern void micro_do_test(abstract_db *db, int argc, char **argv);
extern void micro_insert_do_test(abstract_db *db, int argc, char **argv);
extern void micro_delete_do_test(abstract_db *db, int argc, char **argv);
extern void micro_range_do_test(abstract_db *db, int argc, char **argv);
extern void micro_do_trasitive_test(abstract_db *db, int argc, char **argv);
extern void micro_do_trasitive2_test(abstract_db *db, int argc, char **argv);
extern void micro_do_mem_test(abstract_db *db, int argc, char **argv);
extern void micro_lock_perf_test(abstract_db *db, int argc, char **argv);
extern void micro_ic3_perf_test(abstract_db *db, int argc, char **argv);

//microbench for evalution
extern void microbench_do_test(abstract_db *db, int argc, char **argv);
extern void lockbench_do_test(abstract_db *db, int argc, char **argv);


enum {
  RUNMODE_TIME = 0,
  RUNMODE_OPS  = 1
};

// benchmark global variables
extern size_t nthreads;
extern volatile bool running;
extern int verbose;
extern uint64_t txn_flags;
extern double scale_factor;
extern uint64_t runtime;
extern uint64_t ops_per_worker;
extern int run_mode;
extern int enable_parallel_loading;
extern int pin_cpus;
extern int slow_exit;
extern int retry_aborted_transaction;
extern int no_reset_counters;
extern int backoff_aborted_transaction;
extern int consistency_check;
extern int dynamic_workload;
extern int kid_start;
extern int kid_end;
extern std::vector<std::string> policies_to_eval;
extern double backoff_alpha;

class scoped_db_thread_ctx {
public:
  scoped_db_thread_ctx(const scoped_db_thread_ctx &) = delete;
  scoped_db_thread_ctx(scoped_db_thread_ctx &&) = delete;
  scoped_db_thread_ctx &operator=(const scoped_db_thread_ctx &) = delete;

  scoped_db_thread_ctx(abstract_db *db, bool loader)
    : db(db)
  {
    db->thread_init(loader);
  }
  ~scoped_db_thread_ctx()
  {
    db->thread_end();
  }
private:
  abstract_db *const db;
};

class bench_loader : public ndb_thread {
public:
  bench_loader(unsigned long seed, abstract_db *db,
               const std::map<std::string, abstract_ordered_index *> &open_tables)
    : r(seed), db(db), open_tables(open_tables), b(0)
  {
    txn_obj_buf.reserve(str_arena::MinStrReserveLength);
    txn_obj_buf.resize(db->sizeof_txn_object(txn_flags));
  }
  inline void
  set_barrier(spin_barrier &b)
  {
    ALWAYS_ASSERT(!this->b);
    this->b = &b;
  }
  virtual void
  run()
  {
    { // XXX(stephentu): this is a hack
      scoped_rcu_region r; // register this thread in rcu region
    }
    ALWAYS_ASSERT(b);
    b->count_down();
    b->wait_for();
    scoped_db_thread_ctx ctx(db, true);
    load();
  }
protected:
  inline void *txn_buf() { return (void *) txn_obj_buf.data(); }

  virtual void load() = 0;

  util::fast_random r;
  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;
  spin_barrier *b;
  std::string txn_obj_buf;
  str_arena arena;
};

class bench_worker : public ndb_thread {
public:

  bench_worker(unsigned int worker_id,
               bool set_core_id,
               unsigned long seed, abstract_db *db,
               const std::map<std::string, abstract_ordered_index *> &open_tables,
               spin_barrier *barrier_a, spin_barrier *barrier_b)
    : worker_id(worker_id), set_core_id(set_core_id),
      r(seed), db(db), open_tables(open_tables),
      barrier_a(barrier_a), barrier_b(barrier_b),
      txn_buf_idx(0),
      // the ntxn_* numbers are per worker
      ntxn_commits(0),
      ntxn_commits_new_order(0), 
      ntxn_commits_payment(0), 
      ntxn_commits_delivery(0), 
      ntxn_commits_order_status(0), 
      ntxn_commits_stock_level(0), 
      ntxn_aborts(0),
      ntxn_aborts_new_order(0),
      ntxn_aborts_payment(0),
      ntxn_aborts_delivery(0),
      ntxn_aborts_order_status(0),
      ntxn_aborts_stock_level(0),
      latency_numer_us(0),
      backoff(100), 
      is_user_initiate_abort(false),
      size_delta(0)
  {
    txn_obj_buf_array = new std::string[MAX_TXN_BUF_SIZE]{};
    for (int i = 0; i < MAX_TXN_BUF_SIZE; ++i) {
      txn_obj_buf_array[i].reserve(str_arena::MinStrReserveLength);
      txn_obj_buf_array[i].resize(db->sizeof_txn_object(txn_flags));
    }
    // default worker using the first policy stored inside db
    // for dynamic workload, will be set after policy initialization
    set_pg(db->pg);
    set_policy(db->selector->get_default_policy());
    // check and tune this size
    failed_records.reserve(4);
    tail_latency1.reserve(1 << 12);
    tail_latency2.reserve(1 << 12);
    tail_latency3.reserve(1 << 12);
    tail_latency4.reserve(1 << 12);
    tail_latency5.reserve(1 << 12);
  }

  virtual ~bench_worker() {
    delete[] txn_obj_buf_array;
  }

  // returns [did_commit?, size_increase_bytes]
  typedef std::pair<bool, ssize_t> txn_result;
  typedef txn_result (*txn_fn_t)(bench_worker *);

  struct workload_desc {
    workload_desc() {}
    workload_desc(const std::string &name, double frequency, txn_fn_t fn)
      : name(name), frequency(frequency), fn(fn)
    {
      ALWAYS_ASSERT(frequency >= 0.0);
      ALWAYS_ASSERT(frequency <= 1.0);
    }
    std::string name;
    double frequency;
    txn_fn_t fn;
  };
  typedef std::vector<workload_desc> workload_desc_vec;
  virtual workload_desc_vec get_workload() const = 0;

  virtual void run();

  inline size_t get_ntxn_commits() const { return ntxn_commits; }
  inline size_t get_ntxn_commits_new_order() const { return ntxn_commits_new_order; }
  inline size_t get_ntxn_commits_payment() const { return ntxn_commits_payment; }
  inline size_t get_ntxn_commits_delivery() const { return ntxn_commits_delivery; }
  inline size_t get_ntxn_commits_order_status() const { return ntxn_commits_order_status; }
  inline size_t get_ntxn_commits_stock_level() const { return ntxn_commits_stock_level; }

  inline size_t get_ntxn_aborts() const { return ntxn_aborts; }
  inline size_t get_ntxn_aborts_new_order() const { return ntxn_aborts_new_order; }
  inline size_t get_ntxn_aborts_payment() const { return ntxn_aborts_payment; }
  inline size_t get_ntxn_aborts_delivery() const { return ntxn_aborts_delivery; }
  inline size_t get_ntxn_aborts_order_status() const { return ntxn_aborts_order_status; }
  inline size_t get_ntxn_aborts_stock_level() const { return ntxn_aborts_stock_level; }

  inline uint64_t get_latency_numer_us() const { return latency_numer_us; }
  inline std::vector<uint64_t> get_tail_latency1() const { return tail_latency1; }
  inline std::vector<uint64_t> get_tail_latency2() const { return tail_latency2; }
  inline std::vector<uint64_t> get_tail_latency3() const { return tail_latency3; }
  inline std::vector<uint64_t> get_tail_latency4() const { return tail_latency4; }
  inline std::vector<uint64_t> get_tail_latency5() const { return tail_latency5; }

  inline double
  get_avg_latency_us() const
  {
    return double(latency_numer_us) / double(ntxn_commits);
  }

  std::map<std::string, size_t> get_txn_counts() const;
  std::map<std::string, size_t> get_abort_counts() const;

  typedef abstract_db::counter_map counter_map;
  typedef abstract_db::txn_counter_map txn_counter_map;

#ifdef ENABLE_BENCH_TXN_COUNTERS
  inline txn_counter_map
  get_local_txn_counters() const
  {
    return local_txn_counters;
  }
#endif

  inline ssize_t get_size_delta() const { return size_delta; }

  virtual void final_check() {}

  void increase_commit(size_t txn_idx) {
    ++txn_counts[txn_idx];
  }

  void increase_abort(size_t txn_idx) {
    ++abort_counts[txn_idx];
  }

  void increase_commit() { ntxn_commits++; }
  void increase_commit_new_order() { ntxn_commits_new_order++; }
  void increase_commit_payment() { ntxn_commits_payment++; }
  void increase_commit_delivery() { ntxn_commits_delivery++; }
  void increase_commit_order_status() { ntxn_commits_order_status++; }
  void increase_commit_stock_level() { ntxn_commits_stock_level++; }

  inline void set_policy(Policy *pol) {
    policy = pol;
    txn_obj_buf_array_length = pg->get_txn_buf_size();
    INVARIANT(txn_obj_buf_array_length >= 1 && txn_obj_buf_array_length <= MAX_TXN_BUF_SIZE);
  }

  inline void set_pg(PolicyGradient* p) {
    pg = p;
    txn_obj_buf_array_length = pg->get_txn_buf_size();
  }
  virtual void reset_workload(size_t nthreads, size_t idx) {}

  void clear() {
    txn_buf_idx = 0;
    ntxn_commits = 0;
    ntxn_commits_new_order = 0;
    ntxn_commits_payment = 0;
    ntxn_commits_delivery = 0;
    ntxn_commits_order_status = 0;
    ntxn_commits_stock_level = 0;
    ntxn_aborts = 0;
    ntxn_aborts_new_order = 0;
    ntxn_aborts_payment = 0;
    ntxn_aborts_delivery = 0;
    ntxn_aborts_order_status = 0;
    ntxn_aborts_stock_level = 0;
    latency_numer_us = 0;
    backoff = 100;
    size_delta = 0;
    tail_latency1.clear();
    tail_latency2.clear();
    tail_latency3.clear();
    tail_latency4.clear();
    tail_latency5.clear();
  }

private:
  inline void modify_backoff(AgentDecision backoff_action, double x) {
    switch (backoff_action)
    {
    case agent_inc_backoff:
      backoff = backoff * (1 + x * backoff_alpha);
      backoff = backoff > 6710886400 ? 6710886400 : backoff; // 2^26 = 6710886400
      break;
    case agent_dec_backoff:
      backoff = backoff / (1 + x * backoff_alpha);
      backoff = backoff < 100 ? 100 : backoff;
      break;
    case agent_nop_backoff:
      backoff = backoff;
      break;
    default:
      ALWAYS_ASSERT(false);
    }
  }

protected:

  virtual void on_run_setup() {}

  inline void *txn_buf() {
    txn_buf_idx = (txn_buf_idx + 1) % txn_obj_buf_array_length;
    return (void *) txn_obj_buf_array[txn_buf_idx].data();
  }

  inline void set_failed_records(std::vector<void *> *records) {
    failed_records.clear();
    for (auto it : *records)
	    failed_records.emplace_back(it);
  }

  unsigned int worker_id;
  bool set_core_id;
  util::fast_random r;
  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;
  spin_barrier *const barrier_a;
  spin_barrier *const barrier_b;

private:
  size_t txn_buf_idx;
  size_t ntxn_commits;
  size_t ntxn_commits_new_order;
  size_t ntxn_commits_payment;
  size_t ntxn_commits_delivery;
  size_t ntxn_commits_order_status;
  size_t ntxn_commits_stock_level;
  size_t ntxn_aborts;
  size_t ntxn_aborts_new_order;
  size_t ntxn_aborts_payment;
  size_t ntxn_aborts_delivery;
  size_t ntxn_aborts_order_status;
  size_t ntxn_aborts_stock_level;
  uint64_t latency_numer_us;
  std::vector<uint64_t> tail_latency1;
  std::vector<uint64_t> tail_latency2;
  std::vector<uint64_t> tail_latency3;
  std::vector<uint64_t> tail_latency4;
  std::vector<uint64_t> tail_latency5;
  uint64_t backoff;

protected:
  bool is_user_initiate_abort;

#ifdef ENABLE_BENCH_TXN_COUNTERS
  txn_counter_map local_txn_counters;
  void measure_txn_counters(void *txn, const char *txn_name);
#else
  inline ALWAYS_INLINE void measure_txn_counters(void *txn, const char *txn_name) {}
#endif

  std::vector<size_t> txn_counts; // breakdown of txns
  std::vector<size_t> abort_counts;
  ssize_t size_delta; // how many logical bytes (of values) did the worker add to the DB

//  std::string txn_obj_buf;
  int txn_obj_buf_array_length;
  std::string *txn_obj_buf_array;
  str_arena arena;
  Policy* policy;
  PolicyGradient* pg;
  uint16_t finished_txn_contention;

  std::vector<void *> failed_records;
};


class bench_checker : public ndb_thread {
public:
  bench_checker(abstract_db *db,
               const std::map<std::string, abstract_ordered_index *> &open_tables)
    : db(db), open_tables(open_tables), b(0)
  {
    txn_obj_buf.reserve(str_arena::MinStrReserveLength);
    txn_obj_buf.resize(db->sizeof_txn_object(txn_flags));
  }
  inline void
  set_barrier(spin_barrier &b)
  {
    ALWAYS_ASSERT(!this->b);
    this->b = &b;
  }
  virtual void
  run()
  {
    { // XXX(stephentu): this is a hack
      scoped_rcu_region r; // register this thread in rcu region
    }
    ALWAYS_ASSERT(b);
    b->count_down();
    b->wait_for();
    scoped_db_thread_ctx ctx(db, true);
    check();
  }
protected:
  inline void *txn_buf() { return (void *) txn_obj_buf.data(); }

  virtual void check() = 0;

  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;
  spin_barrier *b;
  std::string txn_obj_buf;
  str_arena arena;
};


class bench_runner {
public:
  bench_runner(const bench_runner &) = delete;
  bench_runner(bench_runner &&) = delete;
  bench_runner &operator=(const bench_runner &) = delete;

  bench_runner(abstract_db *db)
    : db(db), barrier_a(nthreads), barrier_b(1) {}
  virtual ~bench_runner() {}
  void run();
  void dynamic_run();
  void training_run(std::vector<std::string>& policies);
protected:
  // only called once
  virtual std::vector<bench_loader*> make_loaders() = 0;

  // only called once
  virtual std::vector<bench_worker*> make_workers() = 0;

  // only called once
  virtual std::vector<bench_checker*> make_checkers()
  {
    std::vector<bench_checker*>* bw = new std::vector<bench_checker*>();
    return *bw;
  }

  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;

  // barriers for actual benchmark execution
  spin_barrier barrier_a;
  spin_barrier barrier_b;
};

// XXX(stephentu): limit_callback is not optimal, should use
// static_limit_callback if possible
class limit_callback : public abstract_ordered_index::scan_callback {
public:
  limit_callback(ssize_t limit = -1)
    : limit(limit), n(0)
  {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool invoke(
      const char *keyp, size_t keylen,
      const std::string &value)
  {
    INVARIANT(limit == -1 || n < size_t(limit));
    values.emplace_back(std::string(keyp, keylen), value);
    return (limit == -1) || (++n < size_t(limit));
  }

  inline size_t size() const { return values.size(); }
  typedef std::pair<std::string, std::string> kv_pair;
  std::vector<kv_pair> values;

  const ssize_t limit;
private:
  size_t n;
};


class latest_key_callback : public abstract_ordered_index::scan_callback {
public:
  latest_key_callback(std::string &k, ssize_t limit = -1)
    : limit(limit), n(0), k(&k)
  {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool invoke(
      const char *keyp, size_t keylen,
      const std::string &value)
  {
    INVARIANT(limit == -1 || n < size_t(limit));
    k->assign(keyp, keylen);
    ++n;
    return (limit == -1) || (n < size_t(limit));
  }

  inline size_t size() const { return n; }
  inline std::string &kstr() { return *k; }

private:
  ssize_t limit;
  size_t n;
  std::string *k;
};

// explicitly copies keys, because btree::search_range_call() interally
// re-uses a single string to pass keys (so using standard string assignment
// will force a re-allocation b/c of shared ref-counting)
//
// this isn't done for values, because each value has a distinct string from
// the string allocator, so there are no mutations while holding > 1 ref-count
template <size_t N>
class static_limit_callback : public abstract_ordered_index::scan_callback {
public:
  // XXX: push ignore_key into lower layer
  static_limit_callback(str_arena *arena, bool ignore_key)
    : n(0), arena(arena), ignore_key(ignore_key)
  {
    static_assert(N > 0, "xx");
  }

  virtual bool invoke(
      const char *keyp, size_t keylen,
      const std::string &value)
  {
    INVARIANT(n < N);
    INVARIANT(arena->manages(&value));
    if (ignore_key) {
      values.emplace_back(nullptr, &value);
    } else {
      std::string * const s_px = arena->next();
      INVARIANT(s_px && s_px->empty());
      s_px->assign(keyp, keylen);
      values.emplace_back(s_px, &value);
    }
    return ++n < N;
  }

  inline size_t
  size() const
  {
    return values.size();
  }

  typedef std::pair<const std::string *, const std::string *> kv_pair;
  typename util::vec<kv_pair, N>::type values;

private:
  size_t n;
  str_arena *arena;
  bool ignore_key;
};

#endif /* _NDB_BENCH_H_ */
