#ifndef _NDB_WRAPPER_IMPL_H_
#define _NDB_WRAPPER_IMPL_H_

#include <stdint.h>
#include "ndb_wrapper.h"
#include "../counter.h"
#include "../rcu.h"
#include "../varkey.h"
#include "../macros.h"
#include "../util.h"
#include "../scopedperf.hh"
#include "../txn.h"
//#include "../txn_proto1_impl.h"
#include "../txn_ic3_impl.h"
#include "../tuple.h"

struct hint_default_traits : public default_transaction_traits {
  typedef str_arena StringAllocator;
};

// consistency check profiles

struct hint_consistency_check_traits {
  static const size_t read_set_expected_size = 1024;
  static const size_t write_set_expected_size = 1024;
  static const size_t absent_set_expected_size = 1024;
  static const bool stable_input_memory = false;
  static const bool hard_expected_sizes = false;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 1;
  typedef str_arena StringAllocator;
};

// ycsb profiles

struct hint_kv_get_put_traits {
  static const size_t read_set_expected_size = 1;
  static const size_t write_set_expected_size = 1;
  static const size_t absent_set_expected_size = 1;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 1;
  typedef str_arena StringAllocator;
};

struct hint_kv_rmw_traits : public hint_kv_get_put_traits {};

struct hint_kv_scan_traits {
  static const size_t read_set_expected_size = 100;
  static const size_t write_set_expected_size = 1;
  static const size_t absent_set_expected_size = read_set_expected_size / 7 + 1;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 1;
  typedef str_arena StringAllocator;
};

// tpcc profiles

struct hint_read_only_traits {
  static const size_t read_set_expected_size = 1;
  static const size_t write_set_expected_size = 1;
  static const size_t absent_set_expected_size = 1;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 1;
  typedef str_arena StringAllocator;
};

struct hint_tpcc_new_order_traits {
  static const size_t read_set_expected_size = 35;
  static const size_t write_set_expected_size = 35;
  static const size_t absent_set_expected_size = 1;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 35;
  typedef str_arena StringAllocator;
};

struct hint_tpcc_payment_traits {
  static const size_t read_set_expected_size = 85;
  static const size_t write_set_expected_size = 10;
  static const size_t absent_set_expected_size = 15;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 85;
  typedef str_arena StringAllocator;
};

struct hint_tpcc_delivery_traits {
  static const size_t read_set_expected_size = 175;
  static const size_t write_set_expected_size = 175;
  static const size_t absent_set_expected_size = 100;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 85;
  typedef str_arena StringAllocator;
};

struct hint_tpcc_order_status_traits {
  static const size_t read_set_expected_size = 95;
  static const size_t write_set_expected_size = 1;
  static const size_t absent_set_expected_size = 25;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 95;
  typedef str_arena StringAllocator;
};

struct hint_tpcc_order_status_read_only_traits : public hint_read_only_traits {};

struct hint_tpcc_stock_level_traits {
  static const size_t read_set_expected_size = 500;
  static const size_t write_set_expected_size = 1;
  static const size_t absent_set_expected_size = 25;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 1;
  typedef str_arena StringAllocator;
};

struct hint_tpcc_stock_level_read_only_traits : public hint_read_only_traits {};

#define MICROTRAITS 300

struct hint_micro_traits {
  static const size_t read_set_expected_size = MICROTRAITS;
  static const size_t write_set_expected_size = MICROTRAITS;
  static const size_t absent_set_expected_size = 1;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = MICROTRAITS;
  typedef str_arena StringAllocator;
};

struct hint_micro_loader_traits {
  static const size_t read_set_expected_size = 1000;
  static const size_t write_set_expected_size = 1000;
  static const size_t absent_set_expected_size = 1000;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 1000;
  typedef str_arena StringAllocator;
};


struct hint_tpce_broker_volume {
  static const size_t read_set_expected_size = 1000; //TODO
  static const size_t write_set_expected_size = 0;
  static const size_t absent_set_expected_size = 1000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

struct hint_tpce_customer_position {
  static const size_t read_set_expected_size = 1000; //TODO
  static const size_t write_set_expected_size = 0;
  static const size_t absent_set_expected_size = 1000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

struct hint_tpce_market_feed {
  static const size_t read_set_expected_size = 10000; //TODO
  static const size_t write_set_expected_size = 10000; //TODO
  static const size_t absent_set_expected_size = 10000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = false;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

struct hint_tpce_market_watch {
  static const size_t read_set_expected_size = 1000; //TODO
  static const size_t write_set_expected_size = 0;
  static const size_t absent_set_expected_size = 1000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

struct hint_tpce_security_detail {
  static const size_t read_set_expected_size = 1000; //TODO
  static const size_t write_set_expected_size = 0;
  static const size_t absent_set_expected_size = 1000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

struct hint_tpce_trade_lookup {
  static const size_t read_set_expected_size = 1000; //TODO
  static const size_t write_set_expected_size = 0;
  static const size_t absent_set_expected_size = 1000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

struct hint_tpce_trade_order {
  static const size_t read_set_expected_size = 1000; //TODO
  static const size_t write_set_expected_size = 1000; //TODO
  static const size_t absent_set_expected_size = 1000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = false;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

struct hint_tpce_trade_result {
  static const size_t read_set_expected_size = 1000; //TODO
  static const size_t write_set_expected_size = 1000; //TODO
  static const size_t absent_set_expected_size = 1000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = false;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

struct hint_tpce_trade_status {
  static const size_t read_set_expected_size = 1000; //TODO
  static const size_t write_set_expected_size = 0;
  static const size_t absent_set_expected_size = 1000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

struct hint_tpce_trade_update {
  static const size_t read_set_expected_size = 1000; //TODO
  static const size_t write_set_expected_size = 1000; //TODO
  static const size_t absent_set_expected_size = 1000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = false;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

struct hint_seats_traits {
  static const size_t read_set_expected_size = 1000; //TODO
  static const size_t write_set_expected_size = 1000; //TODO
  static const size_t absent_set_expected_size = 1000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

struct hint_seats_read_only_traits {
  static const size_t read_set_expected_size = 1000; //TODO
  static const size_t write_set_expected_size = 0;
  static const size_t absent_set_expected_size = 1000; //TODO
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
  static const size_t dep_queue_expected_size = 64;
  typedef str_arena StringAllocator;
};

#define TXN_PROFILE_HINT_OP(x) \
  x(abstract_db::HINT_DEFAULT, hint_default_traits) \
  x(abstract_db::HINT_CONSISTENCY_CHECK, hint_consistency_check_traits) \
  x(abstract_db::HINT_KV_GET_PUT, hint_kv_get_put_traits) \
  x(abstract_db::HINT_KV_RMW, hint_kv_rmw_traits) \
  x(abstract_db::HINT_KV_SCAN, hint_kv_scan_traits) \
  x(abstract_db::HINT_TPCC_NEW_ORDER, hint_tpcc_new_order_traits) \
  x(abstract_db::HINT_TPCC_PAYMENT, hint_tpcc_payment_traits) \
  x(abstract_db::HINT_TPCC_DELIVERY, hint_tpcc_delivery_traits) \
  x(abstract_db::HINT_TPCC_ORDER_STATUS, hint_tpcc_order_status_traits) \
  x(abstract_db::HINT_TPCC_ORDER_STATUS_READ_ONLY, hint_tpcc_order_status_read_only_traits) \
  x(abstract_db::HINT_TPCC_STOCK_LEVEL, hint_tpcc_stock_level_traits) \
  x(abstract_db::HINT_TPCC_STOCK_LEVEL_READ_ONLY, hint_tpcc_stock_level_read_only_traits) \
  x(abstract_db::HINT_MICRO, hint_micro_traits) \
  x(abstract_db::HINT_MICRO_LOADER, hint_micro_loader_traits) \
  x(abstract_db::HINT_TPCE_BROKER_VOLUME, hint_tpce_broker_volume) \
  x(abstract_db::HINT_TPCE_CUSTOMER_POSITION, hint_tpce_customer_position) \
  x(abstract_db::HINT_TPCE_MARKET_FEED, hint_tpce_market_feed) \
  x(abstract_db::HINT_TPCE_MARKET_WATCH, hint_tpce_market_watch) \
  x(abstract_db::HINT_TPCE_SECURITY_DETAIL, hint_tpce_security_detail) \
  x(abstract_db::HINT_TPCE_TRADE_LOOKUP, hint_tpce_trade_lookup) \
  x(abstract_db::HINT_TPCE_TRADE_ORDER, hint_tpce_trade_order) \
  x(abstract_db::HINT_TPCE_TRADE_RESULT, hint_tpce_trade_result) \
  x(abstract_db::HINT_TPCE_TRADE_STATUS, hint_tpce_trade_status) \
  x(abstract_db::HINT_TPCE_TRADE_UPDATE, hint_tpce_trade_update) \
  x(abstract_db::HINT_SEATS_DEFAULT, hint_seats_traits) \
  x(abstract_db::HINT_SEATS_READ_ONLY_DEFAULT, hint_seats_read_only_traits)

template <template <typename> class Transaction>
ndb_wrapper<Transaction>::ndb_wrapper(
    const std::vector<std::string> &logfiles,
    const std::vector<std::vector<unsigned>> &assignments_given,
    bool call_fsync,
    bool use_compression,
    bool fake_writes,
    PolicySelector *selector,
    PolicyGradient *p) : abstract_db(selector, p)
{
  if (logfiles.empty())
    return;
  std::vector<std::vector<unsigned>> assignments_used;
  txn_logger::Init(
      nthreads, logfiles, assignments_given, &assignments_used,
      call_fsync,
      use_compression,
      fake_writes);
  if (verbose) {
    std::cerr << "[logging subsystem]" << std::endl;
    std::cerr << "  assignments: " << assignments_used << std::endl;
    std::cerr << "  call fsync : " << call_fsync       << std::endl;
    std::cerr << "  compression: " << use_compression  << std::endl;
    std::cerr << "  fake_writes: " << fake_writes      << std::endl;
  }
}

template <template <typename> class Transaction>
size_t
ndb_wrapper<Transaction>::sizeof_txn_object(uint64_t txn_flags) const
{
#define MY_OP_X(a, b) sizeof(typename cast< b >::type),
  const size_t xs[] = {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  };
#undef MY_OP_X
  size_t xmax = 0;
  for (size_t i = 0; i < ARRAY_NELEMS(xs); i++)
    xmax = std::max(xmax, xs[i]);
  return xmax;
}

template <template <typename> class Transaction>
void *
ndb_wrapper<Transaction>::new_txn(
    uint64_t txn_flags,
    str_arena &arena,
    void *buf,
    TxnProfileHint hint)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(buf);
  p->hint = hint;
#define MY_OP_X(a, b) \
  case a: \
    new (&p->buf[0]) typename cast< b >::type(txn_flags, arena); \
    return p;
  switch (hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  return 0;
}

template <template <typename> class Transaction>
void
ndb_wrapper<Transaction>::init_txn(
    void *txn,
    conflict_graph* cg,
    uint8_t type,
    Policy *policy = nullptr,
    PolicyGradient *pg = nullptr)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      t->init(cg, type, policy, pg); \
      return; \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  return;
}

template <typename T>
static inline ALWAYS_INLINE void
Destroy(T *t)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_dtor_probe0_cg);
  t->~T();
}

template <template <typename> class Transaction>
bool
ndb_wrapper<Transaction>::commit_txn(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      const bool ret = t->commit(); \
      Destroy(t); \
      return ret; \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  return false;
}

template <template <typename> class Transaction>
std::pair<bool, uint32_t>
ndb_wrapper<Transaction>::expose_uncommitted(void *txn, uint32_t acc_id = MAX_ACC_ID)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      return t->expose_uncommitted(acc_id); \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  return std::pair<bool, uint32_t>(false, std::numeric_limits<uint32_t>::max());
}

template <template <typename> class Transaction>
void
ndb_wrapper<Transaction>::set_failed_records(void *txn, std::vector<void *>& records)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      t->set_clean_read_failed_tuples(records); \
      return; \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
}

template <template <typename> class Transaction>
std::vector<void *> *
ndb_wrapper<Transaction>::get_failed_records(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      return t->get_clean_read_failed_tuples(); \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  return nullptr;
}

template <template <typename> class Transaction>
uint16_t
ndb_wrapper<Transaction>::get_txn_contention(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      return t->get_txn_contention(); \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  return 0;
}

template <template <typename> class Transaction>
uint32_t
ndb_wrapper<Transaction>::get_write_number(void* txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      return t->get_write_number(); \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  ALWAYS_ASSERT(false);
  return 0;
}


template <template <typename> class Transaction>
bool
ndb_wrapper<Transaction>::should_abort(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      return t->should_abort(); \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
}

template <template <typename> class Transaction>
void
ndb_wrapper<Transaction>::abort_txn(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      t->abort(); \
      Destroy(t); \
      return; \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
}

template <template <typename> class Transaction>
void
ndb_wrapper<Transaction>::one_op_begin(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      t->atomic_piece_begin(true); \
      return; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}


template <template <typename> class Transaction>
bool
ndb_wrapper<Transaction>::one_op_end(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      return t->atomic_piece_end(); \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
    return false;
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return false;
}


template <template <typename> class Transaction>
void
ndb_wrapper<Transaction>::mul_ops_begin(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      t->atomic_piece_begin(false); \
      return; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

template <template <typename> class Transaction>
bool
ndb_wrapper<Transaction>::mul_ops_end(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      return t->atomic_piece_end(); \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
    return false;
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return false;
}


template <template <typename> class Transaction>
bool
ndb_wrapper<Transaction>::atomic_ops_abort(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      return t->atomic_piece_abort(); \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
    return false;
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return false;
}

template <template <typename> class Transaction>
void
ndb_wrapper<Transaction>::print_txn_debug(void *txn) const
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      t->dump_debug_info(); \
      return; \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
}

template <template <typename> class Transaction>
std::map<std::string, uint64_t>
ndb_wrapper<Transaction>::get_txn_counters(void *txn) const
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      return t->get_txn_counters(); \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  return std::map<std::string, uint64_t>();
}

//debug purpose
template <template <typename> class Transaction>
uint64_t
ndb_wrapper<Transaction>::get_tid(
    void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      return t->get_tid(); \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  return 0;
}


template <template <typename> class Transaction>
abstract_ordered_index *
ndb_wrapper<Transaction>::open_index(const std::string &name, size_t value_size_hint, bool mostly_append)
{
  return new ndb_ordered_index<Transaction>(name, value_size_hint, mostly_append);
}

template <template <typename> class Transaction>
void
ndb_wrapper<Transaction>::close_index(abstract_ordered_index *idx)
{
  delete idx;
}

template <template <typename> class Transaction>
ndb_ordered_index<Transaction>::ndb_ordered_index(
    const std::string &name, size_t value_size_hint, bool mostly_append)
  : name(name), btr(value_size_hint, mostly_append, name)
{
  // for debugging
  //std::cerr << name << " : btree= "
  //          << btr.get_underlying_btree()
  //          << std::endl;
}

template <template <typename> class Transaction>
bool
ndb_ordered_index<Transaction>::get(
    void *txn,
    const std::string &key,
    std::string &value,
    size_t max_bytes_read,
    uint32_t acc_id)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_get_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      if (!btr.search(*t, key, value, max_bytes_read, acc_id)) \
        return false; \
      return true; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
    INVARIANT(!value.empty());
    return true;
  } catch (piece_abort_exception &ex) {
    throw ex;

  }
}



template <template <typename> class Transaction>
bool
ndb_ordered_index<Transaction>::get_profile(
    void *txn,
      const std::string &key,
      std::string &value,
      size_t max_bytes_read,
      ic3_profile* prof) 
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_get_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      if (!btr.profile_search(*t, key, value, max_bytes_read, prof)) \
        return false; \
      return true; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
    INVARIANT(!value.empty());
    return true;
  } catch (piece_abort_exception &ex) {
    throw ex;

  }
}

// XXX: find way to remove code duplication below using C++ templates!

template <template <typename> class Transaction>
const char *
ndb_ordered_index<Transaction>::put(
    void *txn,
    const std::string &key,
    const std::string &value,
    uint32_t acc_id)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_put_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.put(*t, key, value, acc_id); \
      return 0; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return 0;
}

template <template <typename> class Transaction>
const char *
ndb_ordered_index<Transaction>::put(
    void *txn,
    std::string &&key,
    std::string &&value,
    uint32_t acc_id)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.put(*t, std::move(key), std::move(value), acc_id); \
      return 0; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return 0;
}

template <template <typename> class Transaction>
const char *
ndb_ordered_index<Transaction>::update(
    void *txn,
    const std::string &key,
    std::string &value,
    update_callback *callback,
    uint32_t acc_id)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_put_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.update(*t, key, value, callback, acc_id); \
      return 0; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
      default:
        ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return 0;
}

template <template <typename> class Transaction>
void
ndb_ordered_index<Transaction>::commutative_act(
    void *txn,
    const std::string &key,
    dbtuple::tuple_commutative_act act,
    uint64_t param)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.commutative_act(*t, key, act, param); \
      return; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return;
}

template <template <typename> class Transaction>
const char *
ndb_ordered_index<Transaction>::insert(
    void *txn,
    const std::string &key,
    const std::string &value,
    uint32_t acc_id)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_insert_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.insert(*t, key, value, acc_id); \
      return 0; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return 0;
}

template <template <typename> class Transaction>
const char *
ndb_ordered_index<Transaction>::insert(
    void *txn,
    std::string &&key,
    std::string &&value,
    uint32_t acc_id)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.insert(*t, std::move(key), std::move(value), acc_id); \
      return 0; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return 0;
}

template <template <typename> class Transaction>
class ndb_wrapper_search_range_callback : public txn_btree<Transaction>::search_range_callback {
public:
  ndb_wrapper_search_range_callback(abstract_ordered_index::scan_callback &upcall)
    : upcall(&upcall) {}

  virtual bool
  invoke(const typename txn_btree<Transaction>::keystring_type &k,
         const typename txn_btree<Transaction>::string_type &v)
  {
    return upcall->invoke(k.data(), k.length(), v);
  }

private:
  abstract_ordered_index::scan_callback *upcall;
};

template <template <typename> class Transaction>
void
ndb_ordered_index<Transaction>::scan(
    void *txn,
    const std::string &start_key,
    const std::string *end_key,
    scan_callback &callback,
    str_arena *arena,
    uint32_t acc_id)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_scan_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  ndb_wrapper_search_range_callback<Transaction> c(callback);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.search_range_call(*t, start_key, end_key, c, acc_id); \
      return; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

template <template <typename> class Transaction>
void
ndb_ordered_index<Transaction>::rscan(
    void *txn,
    const std::string &start_key,
    const std::string *end_key,
    scan_callback &callback,
    str_arena *arena,
    uint32_t acc_id)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  ndb_wrapper_search_range_callback<Transaction> c(callback);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.rsearch_range_call(*t, start_key, end_key, c, acc_id); \
      return; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

template <template <typename> class Transaction>
void
ndb_ordered_index<Transaction>::remove(void *txn,
                                       const std::string &key,
                                       uint32_t acc_id)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_remove_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.remove(*t, key, acc_id); \
      return; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

template <template <typename> class Transaction>
void
ndb_ordered_index<Transaction>::remove(void *txn, std::string &&key,
                                       uint32_t acc_id)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.remove(*t, std::move(key), acc_id); \
      return; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

template <template <typename> class Transaction>
size_t
ndb_ordered_index<Transaction>::size() const
{
  return btr.size_estimate();
}

template <template <typename> class Transaction>
std::map<std::string, uint64_t>
ndb_ordered_index<Transaction>::clear()
{
#ifdef TXN_BTREE_DUMP_PURGE_STATS
  std::cerr << "purging txn index: " << name << std::endl;
#endif
  return btr.unsafe_purge(true);
}

#endif /* _NDB_WRAPPER_IMPL_H_ */
