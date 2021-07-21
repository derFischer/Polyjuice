#ifndef _NDB_TXN_H_
#define _NDB_TXN_H_

#include <malloc.h>
#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>

#include <map>
#include <iostream>
#include <vector>
#include <string>
#include <utility>
#include <stdexcept>
#include <limits>
#include <type_traits>
#include <tuple>

#include <unordered_map>

#include "amd64.h"
#include "btree_choice.h"
#include "core.h"
#include "counter.h"
#include "macros.h"
#include "varkey.h"
#include "util.h"
#include "rcu.h"
#include "thread.h"
#include "spinlock.h"
#include "small_unordered_map.h"
#include "static_unordered_map.h"
#include "static_vector.h"
#include "prefetch.h"
#include "tuple.h"
#include "scopedperf.hh"
#include "marked_ptr.h"
#include "ndb_type_traits.h"
#include "piece.h"
#include "txn_entry.h"
#include "conflict_graph.h"
#include "policy.h"
#include "PolicyGradient.h"
#include "update_callback.h"

// forward decl
template <template <typename> class Transaction, typename P>
  class base_txn_btree;

class transaction_unusable_exception {};
class transaction_read_only_exception {};

// XXX: hacky
extern std::string (*g_proto_version_str)(uint64_t v);


// base class with very simple definitions- nothing too exciting yet
class transaction_base {
  template <template <typename> class T, typename P>
    friend class base_txn_btree;
public:

  typedef dbtuple::tid_t tid_t;
  typedef dbtuple::size_type size_type;
  typedef dbtuple::string_type string_type;

  // TXN_EMBRYO - the transaction object has been allocated but has not
  // done any operations yet
  enum txn_state { TXN_EMBRYO, TXN_ACTIVE, TXN_FINISH_EXEC, TXN_COMMITED, TXN_ABRT, };

  enum piece_state {rendezvous_phase = 1, serialization_phase, execution_phase,};

  enum {
    // use the low-level scan protocol for checking scan consistency,
    // instead of keeping track of absent ranges
    TXN_FLAG_LOW_LEVEL_SCAN = 0x1,

    // true to mark a read-only transaction- if a txn marked read-only
    // does a write, a transaction_read_only_exception is thrown and the
    // txn is aborted
    TXN_FLAG_READ_ONLY = 0x2,

    // XXX: more flags in the future, things like consistency levels
  };

#define ABORT_REASONS(x) \
    x(ABORT_REASON_NONE) \
    x(ABORT_REASON_USER) \
    x(ABORT_REASON_CASCADING) \
    x(ABORT_REASON_UNSTABLE_READ) \
    x(ABORT_REASON_FUTURE_TID_READ) \
    x(ABORT_REASON_NODE_SCAN_WRITE_VERSION_CHANGED) \
    x(ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED) \
    x(ABORT_REASON_WRITE_NODE_INTERFERENCE) \
    x(ABORT_REASON_INSERT_NODE_INTERFERENCE) \
    x(ABORT_REASON_READ_NODE_INTEREFERENCE) \
    x(ABORT_REASON_READ_ABSENCE_INTEREFERENCE) \
    x(ABORT_REASON_TIMEOUT) \
    x(ABORT_REASON_EARLY_VALIDATION)

  enum abort_reason {
#define ENUM_X(x) x,
    ABORT_REASONS(ENUM_X)
#undef ENUM_X
  };

  static const char *
  AbortReasonStr(abort_reason reason)
  {
    switch (reason) {
#define CASE_X(x) case x: return #x;
    ABORT_REASONS(CASE_X)
#undef CASE_X
    default:
      break;
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

  transaction_base(uint64_t flags)
    : state(TXN_EMBRYO),
      reason(ABORT_REASON_NONE),
      flags(flags),
      init(false) {}

  transaction_base(const transaction_base &) = delete;
  transaction_base(transaction_base &&) = delete;
  transaction_base &operator=(const transaction_base &) = delete;

protected:
#define EVENT_COUNTER_DEF_X(x) \
  static event_counter g_ ## x ## _ctr;
  ABORT_REASONS(EVENT_COUNTER_DEF_X)
#undef EVENT_COUNTER_DEF_X

  static event_counter *
  AbortReasonCounter(abort_reason reason)
  {
    switch (reason) {
#define EVENT_COUNTER_CASE_X(x) case x: return &g_ ## x ## _ctr;
    ABORT_REASONS(EVENT_COUNTER_CASE_X)
#undef EVENT_COUNTER_CASE_X
    default:
      break;
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

public:

  // only fires during invariant checking
  inline void
  ensure_active()
  {
    if (state == TXN_EMBRYO)
      state = TXN_ACTIVE;
    //INVARIANT(state == TXN_ACTIVE);
  }

  inline uint64_t
  get_flags() const
  {
    return flags;
  }

  inline void finish_init()
  {
    init = true;
  }

  inline bool is_init()
  {
    return init;
  }
protected:

  // the read set is a mapping from (tuple -> tid_read).
  // "write_set" is used to indicate if this read tuple
  // also belongs in the write set.
  struct read_record_t {
    //constexpr read_record_t() : tuple(), t() {}
    constexpr read_record_t(const dbtuple *tuple, tid_t cur_pid, uint64_t pid, uint64_t tid, void* txn, uint64_t clean_tid, bool occ_ = false, uint16_t record_contention = 0, access_entry *re = nullptr)
      : tuple(tuple), t(cur_pid), entry(re), clean_t(clean_tid), occ(occ_), dep_txn(nullptr), dep_tid(0), dep_write_seq(-1), contention(record_contention) {}
    inline const dbtuple *
    get_tuple() const
    {
      return tuple;
    }
    inline tid_t
    get_tid() const
    {
      return t;
    }
    inline access_entry*
    get_access_entry()
    {
      return entry;
    }
    inline void
    set_access_entry(access_entry* e)
    {
      entry = e;
    }
    inline void
    reset_tid(tid_t cur_pid)
    {
      t = cur_pid;
    }
    inline bool
    is_occ()
    {
      return occ;
    }
    inline void
    set_dependent(void* txn, tid_t tid, int seq)
    {
      dep_txn = txn;
      dep_tid = tid;
      dep_write_seq = seq;
    }
    inline void*
    get_dep_txn()
    {
      ALWAYS_ASSERT(!occ);
      return dep_txn;
    }
    inline tid_t
    get_dep_tid()
    {
      ALWAYS_ASSERT(!occ);
      return dep_tid;
    }
    inline int
    get_dep_write_seq()
    {
      ALWAYS_ASSERT(dep_write_seq >= 0);
      return dep_write_seq;
    }
    inline uint64_t
    clean_tid()
    {
      // XXX(Jiachen):return the clean tid of the tuple for clean read validation
      // 1. OCC get action validation
      // 2. IC3 get action (no dirty version found) validation
      return clean_t;
    }
    inline void
    set_clean_tid(tid_t tid)
    {
      clean_t = tid;
    }
    inline uint16_t
    get_contention()
    {
      return contention;
    }
  private:
    const dbtuple *tuple;
    tid_t t;
    access_entry *entry;
    tid_t clean_t;
    bool occ;
    void* dep_txn;
    tid_t dep_tid;
    int dep_write_seq;
    uint16_t contention;
  };

  friend std::ostream &
  operator<<(std::ostream &o, const read_record_t &r);

  // the write set is logically a mapping from (tuple -> value_to_write).
  struct write_record_t {
    enum {
      FLAGS_INSERT  = 0x1,
      FLAGS_DOWRITE = 0x1 << 1,
    };

    constexpr inline write_record_t()
      : tuple(), k(), n(), r(), w(), btr(), entry(nullptr), read_entry(nullptr), occ(false), contention(0)
    {}

    // all inputs are assumed to be stable
    inline write_record_t(dbtuple *tuple,
                          const string_type *k,
                          concurrent_btree::node_opaque_t *n,
                          const void *r,
                          dbtuple::tuple_writer_t w,
                          concurrent_btree *btr,
                          bool insert,
                          access_entry* e,
                          uint64_t pid,
                          uint64_t tid,
                          void* txn,
                          bool occ,
                          uint16_t record_contention,
                          access_entry *we = nullptr)
      : tuple(tuple),
        k(k),
        n(n),
        r(r),
        w(w),
        btr(btr),
        entry(we),
        read_entry(e),
        occ(occ),
        contention(record_contention)
    {
      this->btr.set_flags(insert ? FLAGS_INSERT : 0);
    }
    inline dbtuple *
    get_tuple()
    {
//      dbtuple* res = (dbtuple*)entry.get_commit_target();
//      if(res)
//        return res;

      return tuple; 
    }
    inline const dbtuple *
    get_tuple() const
    {
//      dbtuple* res = (dbtuple*)entry.get_commit_target();
//      if(res)
//        return res;

      return tuple; 

    }
    inline bool
    is_insert() const
    {
      return btr.get_flags() & FLAGS_INSERT;
    }
    inline bool
    do_write() const
    {
      return btr.get_flags() & FLAGS_DOWRITE;
    }
    inline void
    set_do_write()
    {
      INVARIANT(!do_write());
      btr.or_flags(FLAGS_DOWRITE);
    }
    inline void
    clear_do_write()
    {
      btr.clear_flags(FLAGS_DOWRITE);
    }
    inline concurrent_btree *
    get_btree() const
    {
      return btr.get();
    }
    inline const string_type &
    get_key() const
    {
      return *k;
    }
    inline void *
    get_value() const
    {
//      return (const void*)entry.get_value();
      return r;
    }
    inline dbtuple::tuple_writer_t
    get_writer() const
    {
      return w;
    }

    inline access_entry*
    get_access_entry()
    {
      return entry;
    }
    inline void
    set_access_entry(access_entry* e)
    {
      entry = e;
    }
    inline access_entry*
    get_read_access_entry()
    {
      return read_entry;
    }
    inline void
    set_read_access_entry(access_entry* e)
    {
      read_entry = e;
    }
    inline bool
    is_occ()
    {
      return occ;
    }
    // XXX(Conrad): Can use this to compare write_record_t directly
    inline ALWAYS_INLINE
    bool operator<(const write_record_t &o) const
    {
      // the unique key is [tuple, !is_insert, pos]
      return tuple < o.tuple ||
             (tuple == o.tuple && !is_insert() < !o.is_insert());
    }

    inline uint16_t
    get_contention()
    {
      return contention;
    }
    inline concurrent_btree::node_opaque_t *
    get_insert_node()
    {
      return n;
    }
  private:
    dbtuple *tuple;
    const string_type *k;
    concurrent_btree::node_opaque_t *n;
    const void *r;
    dbtuple::tuple_writer_t w;
    marked_ptr<concurrent_btree> btr; // first bit for inserted, 2nd for dowrite
    access_entry* entry;
    access_entry* read_entry;
    bool occ;
    uint16_t contention;
  };


  struct insert_record_t {

    insert_record_t(typename concurrent_btree::node_opaque_t * n, const std::string* k)
    {
      node = n;
      key = k;
    }

    typename concurrent_btree::node_opaque_t * node;
    const std::string* key;
  };

  struct commutative_record_t {

    constexpr inline commutative_record_t()
      : tuple(), k(), act(nullptr), param(0), btr(), w()
    {}

    
    // all inputs are assumed to be stable
    inline commutative_record_t(dbtuple *tuple,
                                const string_type *k,
                                dbtuple::tuple_commutative_act a,
                                uint64_t p,
                                concurrent_btree* btr,
                                dbtuple::tuple_writer_t w)
      : tuple(tuple),
        k(k),
        act(a),
        param(p),
        btr(btr),
        w(w)
    {}

    inline ALWAYS_INLINE
    bool operator<(const commutative_record_t &o) const
    {
      return tuple < o.tuple;
    }

    inline dbtuple::tuple_writer_t
    get_writer() const
    {
      return w;
    }
    
  public:
    dbtuple *tuple;
    const string_type *k;
    dbtuple::tuple_commutative_act act;
    uint64_t param;
    marked_ptr<concurrent_btree> btr;
    dbtuple::tuple_writer_t w;
    mcslock::qnode_t m_node;

  };

  friend std::ostream &
  operator<<(std::ostream &o, const write_record_t &r);

  // the absent set is a mapping from (btree_node -> version_number).
  struct absent_record_t { 
    uint64_t version; 
    const std::string* low_key;
    const std::string* up_key;
    bool occ = false;
  };

  friend std::ostream &
  operator<<(std::ostream &o, const absent_record_t &r);

    struct dbtuple_read_write_info {
    enum {
      FLAGS_LOCKED = 0x1,
      FLAGS_IS_WRITE = 0x1 << 2,
    };
    dbtuple_read_write_info() : tuple(), mcslock(), w_entry(nullptr), r_entry(nullptr), pos() {}
    dbtuple_read_write_info(dbtuple *tuple, write_record_t *w_entry, read_record_t *r_entry,
                            bool is_write, size_t pos)
      : tuple(tuple), mcslock(tuple->m_lock), w_entry(w_entry), r_entry(r_entry), pos(pos)
    {
      if (is_write)
        this->tuple.or_flags(FLAGS_IS_WRITE);
    }
    inline dbtuple *
    get_tuple()
    {
      return tuple.get();
    }
    inline const dbtuple *
    get_tuple() const
    {
      return tuple.get();
    }
    inline ALWAYS_INLINE void
    mark_locked()
    {
      tuple.or_flags(FLAGS_LOCKED);
    }
    inline ALWAYS_INLINE bool
    is_locked() const
    {
      return tuple.get_flags() & FLAGS_LOCKED;
    }
    inline ALWAYS_INLINE bool
    is_write() const
    {
      return tuple.get_flags() & FLAGS_IS_WRITE;
    }
    inline ALWAYS_INLINE
    bool operator<(const dbtuple_read_write_info &o) const
    {
      // sorted the dbtuples in the order of mcs_lock addressed
      return mcslock < o.mcslock ||
             (mcslock == o.mcslock && is_write() < o.is_write()) ||
             (mcslock == o.mcslock && is_write() == o.is_write() && pos < o.pos);
    }
    marked_ptr<dbtuple> tuple;
    marked_ptr<mcslock> mcslock;
    write_record_t *w_entry;
    read_record_t *r_entry;
    size_t pos;
  };

  struct dbtuple_write_info {
    enum {
      FLAGS_LOCKED = 0x1,
      FLAGS_INSERT = 0x1 << 1,
    };
    dbtuple_write_info() : tuple(), mcslock(), entry(nullptr), pos() {}
    dbtuple_write_info(dbtuple *tuple, write_record_t *entry,
                       bool is_insert, size_t pos)
      : tuple(tuple), mcslock(tuple->m_lock), entry(entry), pos(pos)
    {
      // insert does not mean lock is on
      if (is_insert)
        this->tuple.set_flags(FLAGS_INSERT);
//        this->tuple.set_flags(FLAGS_LOCKED | FLAGS_INSERT);
    }
    // XXX: for searching only
    explicit dbtuple_write_info(const dbtuple *tuple)
      : tuple(const_cast<dbtuple *>(tuple)), entry(), pos() {}
    inline dbtuple *
    get_tuple()
    {
      return tuple.get();
    }
    inline const dbtuple *
    get_tuple() const
    {
      return tuple.get();
    }
    inline ALWAYS_INLINE void
    mark_locked()
    {
      // INVARIANT(!is_locked());
      tuple.or_flags(FLAGS_LOCKED);
      // INVARIANT(is_locked());
    }
    inline ALWAYS_INLINE bool
    is_locked() const
    {
      return tuple.get_flags() & FLAGS_LOCKED;
    }
    inline ALWAYS_INLINE bool
    is_insert() const
    {
      return tuple.get_flags() & FLAGS_INSERT;
    }
    inline ALWAYS_INLINE
    bool operator<(const dbtuple_write_info &o) const
    {
#ifdef USE_MCS_LOCK
      // sorted the dbtuples in the order of mcs_lock addressed
      return mcslock < o.mcslock ||
             (mcslock == o.mcslock && !is_insert() < !o.is_insert()) ||
             (mcslock == o.mcslock && !is_insert() == !o.is_insert() && pos < o.pos);
#else
      // the unique key is [tuple, !is_insert, pos]
      return tuple < o.tuple ||
             (tuple == o.tuple && !is_insert() < !o.is_insert()) ||
             (tuple == o.tuple && !is_insert() == !o.is_insert() && pos < o.pos);
#endif
    }
    marked_ptr<dbtuple> tuple;
    marked_ptr<mcslock> mcslock;
    write_record_t *entry;
    size_t pos;
  };

  struct dbtuple_read_info {
    enum {
      FLAGS_LOCKED = 0x1,
      FLAGS_INSERT = 0x1 << 1,
    };
    dbtuple_read_info() : tuple(), entry(nullptr), pos() {}
    dbtuple_read_info(dbtuple *tuple, read_record_t *entry,
                       bool is_insert, size_t pos)
      : tuple(tuple), entry(entry), pos(pos)
    {
      if (is_insert)
        this->tuple.set_flags(FLAGS_LOCKED | FLAGS_INSERT);
    }
    // XXX: for searching only
    explicit dbtuple_read_info(const dbtuple *tuple)
      : tuple(const_cast<dbtuple *>(tuple)), entry(), pos() {}
    inline dbtuple *
    get_tuple()
    {
      return tuple.get();
    }
    inline const dbtuple *
    get_tuple() const
    {
      return tuple.get();
    }
    inline ALWAYS_INLINE void
    mark_locked()
    {
      INVARIANT(!is_locked());
      tuple.or_flags(FLAGS_LOCKED);
      INVARIANT(is_locked());
    }
    inline ALWAYS_INLINE bool
    is_locked() const
    {
      return tuple.get_flags() & FLAGS_LOCKED;
    }
    inline ALWAYS_INLINE bool
    is_insert() const
    {
      return tuple.get_flags() & FLAGS_INSERT;
    }
    inline ALWAYS_INLINE
    bool operator<(const dbtuple_read_info &o) const
    {
      // the unique key is [tuple, !is_insert, pos]
      return tuple < o.tuple ||
             (tuple == o.tuple && !is_insert() < !o.is_insert()) ||
             (tuple == o.tuple && !is_insert() == !o.is_insert() && pos < o.pos);
    }
    marked_ptr<dbtuple> tuple;
    read_record_t *entry;
    size_t pos;
  };


  static event_counter g_evt_read_logical_deleted_node_search;
  static event_counter g_evt_read_logical_deleted_node_scan;
  static event_counter g_evt_dbtuple_write_search_failed;
  static event_counter g_evt_dbtuple_write_insert_failed;

  static event_counter evt_local_search_lookups;
  static event_counter evt_local_search_write_set_hits;
  static event_counter evt_dbtuple_latest_replacement;

  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe0, g_txn_commit_probe0_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe1, g_txn_commit_probe1_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe2, g_txn_commit_probe2_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe3, g_txn_commit_probe3_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe4, g_txn_commit_probe4_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe5, g_txn_commit_probe5_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe6, g_txn_commit_probe6_cg);

  volatile txn_state state;
  abort_reason reason;
  const uint64_t flags;
  volatile bool init;
};

// type specializations
namespace private_ {
  template <>
  struct is_trivially_destructible<transaction_base::read_record_t> {
    static const bool value = true;
  };

  template <>
  struct is_trivially_destructible<transaction_base::write_record_t> {
    static const bool value = true;
  };

  template <>
  struct is_trivially_destructible<transaction_base::absent_record_t> {
    static const bool value = true;
  };

  template <>
  struct is_trivially_destructible<transaction_base::dbtuple_write_info> {
    static const bool value = true;
  };
}

inline ALWAYS_INLINE std::ostream &
operator<<(std::ostream &o, const transaction_base::read_record_t &r)
{
  //o << "[tuple=" << util::hexify(r.get_tuple())
  o << "[tuple=" << *r.get_tuple()
    << ", tid_read=" << g_proto_version_str(r.get_tid())
    << "]";
  return o;
}

inline ALWAYS_INLINE std::ostream &
operator<<(
    std::ostream &o,
    const transaction_base::write_record_t &r)
{
  o << "[tuple=" << r.get_tuple()
    << ", key=" << util::hexify(r.get_key())
    //<< ", value=" << util::hexify(r.get_value())
    << ", insert=" << r.is_insert()
    << ", do_write=" << r.do_write()
    << ", btree=" << r.get_btree()
    << "]";
  return o;
}

inline ALWAYS_INLINE std::ostream &
operator<<(std::ostream &o, const transaction_base::absent_record_t &r)
{
  o << "[v=" << r.version << "]";
  return o;
}

struct default_transaction_traits {
  static const size_t read_set_expected_size = SMALL_SIZE_MAP;
  static const size_t absent_set_expected_size = EXTRA_SMALL_SIZE_MAP;
  static const size_t write_set_expected_size = SMALL_SIZE_MAP;
  static const bool stable_input_memory = false;
  static const bool hard_expected_sizes = true; // true if the expected sizes are hard maximums
  static const bool read_own_writes = true; // if we read a key which we previous put(), are we guaranteed
                                            // to read our latest (uncommited) values? this comes at a
                                            // performance penality [you should not need this behavior to
                                            // write txns, since you *know* the values you inserted]

  static const size_t dep_queue_expected_size = SMALL_SIZE_MAP;

  typedef util::default_string_allocator StringAllocator;
};

struct default_stable_transaction_traits : public default_transaction_traits {
  static const bool stable_input_memory = true;
};

template <template <typename> class Protocol, typename Traits>
class transaction : public transaction_base {
  // XXX: weaker than necessary
  template <template <typename> class, typename>
    friend class base_txn_btree;
  friend Protocol<Traits>;

  friend class atomic_mul_ops<transaction<Protocol, Traits>>;
  friend class atomic_one_op<transaction<Protocol, Traits>>;
  friend class mix_op<transaction<Protocol, Traits>>;

  static const uint32_t CSTEP_SHIFT = 32;
  static const uint32_t VSTEP_SHIFT = 0;

public:

  // KeyWriter is expected to implement:
  // [1-arg constructor]
  //   KeyWriter(const Key *)
  // [fully materialize]
  //   template <typename StringAllocator>
  //   const std::string * fully_materialize(bool, StringAllocator &)

  // ValueWriter is expected to implement:
  // [1-arg constructor]
  //   ValueWriter(const Value *, ValueInfo)
  // [compute new size from old value]
  //   size_t compute_needed(const uint8_t *, size_t)
  // [fully materialize]
  //   template <typename StringAllocator>
  //   const std::string * fully_materialize(bool, StringAllocator &)
  // [perform write]
  //   void operator()(uint8_t *, size_t)
  //
  // ValueWriter does not have to be move/copy constructable. The value passed
  // into the ValueWriter constructor is guaranteed to be valid throughout the
  // lifetime of a ValueWriter instance.

  // KeyReader Interface
  //
  // KeyReader is a simple transformation from (const std::string &) => const Key &.
  // The input is guaranteed to be stable, so it has a simple interface:
  //
  //   const Key &operator()(const std::string &)
  //
  // The KeyReader is expect to preserve the following property: After a call
  // to operator(), but before the next, the returned value is guaranteed to be
  // valid and remain stable.

  // ValueReader Interface
  //
  // ValueReader is a more complex transformation from (const uint8_t *, size_t) => Value &.
  // The input is not guaranteed to be stable, so it has a more complex interface:
  //
  //   template <typename StringAllocator>
  //   bool operator()(const uint8_t *, size_t, StringAllocator &)
  //
  // This interface returns false if there was not enough buffer space to
  // finish the read, true otherwise.  Note that this interface returning true
  // does NOT mean that a read was stable, but it just means there were enough
  // bytes in the buffer to perform the tentative read.
  //
  // Note that ValueReader also exposes a dup interface
  //
  //   template <typename StringAllocator>
  //   void dup(const Value &, StringAllocator &)
  //
  // ValueReader also exposes a means to fetch results:
  //
  //   Value &results()
  //
  // The ValueReader is expected to preserve the following property: After a
  // call to operator(), if it returns true, then the value returned from
  // results() should remain valid and stable until the next call to
  // operator().

  //typedef typename P::Key key_type;
  //typedef typename P::Value value_type;
  //typedef typename P::ValueInfo value_info_type;

  //typedef typename P::KeyWriter key_writer_type;
  //typedef typename P::ValueWriter value_writer_type;

  //typedef typename P::KeyReader key_reader_type;
  //typedef typename P::SingleValueReader single_value_reader_type;
  //typedef typename P::ValueReader value_reader_type;

  typedef Traits traits_type;
  typedef typename Traits::StringAllocator string_allocator_type;

protected:
  // data structures

  inline ALWAYS_INLINE Protocol<Traits> *
  cast()
  {
    return static_cast<Protocol<Traits> *>(this);
  }

  inline ALWAYS_INLINE const Protocol<Traits> *
  cast() const
  {
    return static_cast<const Protocol<Traits> *>(this);
  }

  // XXX: we have baked in b-tree into the protocol- other indexes are possible
  // but we would need to abstract it away. we don't bother for now.

#ifdef USE_SMALL_CONTAINER_OPT
  // XXX: use parameterized typedef to avoid duplication

  // small types
  typedef small_vector<
    read_record_t,
    traits_type::read_set_expected_size> read_set_map_small;
  typedef small_vector<
    access_entry,
    traits_type::read_set_expected_size> read_access_entry_map_small;
  typedef small_vector<
    write_record_t,
    traits_type::write_set_expected_size> write_set_map_small;
  typedef small_vector<
    access_entry,
    traits_type::write_set_expected_size> write_access_entry_map_small;
  typedef small_unordered_map<
    const typename concurrent_btree::node_opaque_t *, absent_record_t,
    traits_type::absent_set_expected_size> absent_set_map_small;
  typedef small_vector<
    txn_wait_entry,
    traits_type::dep_queue_expected_size> dep_queue_map_small;  
  typedef small_vector<
    int,
    traits_type::read_set_expected_size> read_unexposed_map_small;
  typedef small_vector<
    int,
    traits_type::write_set_expected_size> write_unexposed_map_small;
  

  // static types
  typedef static_vector<
    read_record_t,
    traits_type::read_set_expected_size> read_set_map_static;
  typedef static_vector<
    access_entry,
    traits_type::read_set_expected_size> read_access_entry_static;
  typedef static_vector<
    write_record_t,
    traits_type::write_set_expected_size> write_set_map_static;
  typedef static_vector<
    access_entry,
    traits_type::write_set_expected_size> write_access_entry_static;
  typedef static_unordered_map<
    const typename concurrent_btree::node_opaque_t *, absent_record_t,
    traits_type::absent_set_expected_size> absent_set_map_static;
  typedef static_vector<
    txn_wait_entry,
    traits_type::dep_queue_expected_size> dep_queue_map_static;
  typedef static_vector<
    int,
    traits_type::read_set_expected_size> read_unexposed_map_static;
  typedef static_vector<
    int,
    traits_type::write_set_expected_size> write_unexposed_map_static;
  
  // helper types for log writing
  typedef small_vector<
    uint32_t,
    traits_type::write_set_expected_size> write_set_u32_vec_small;
  typedef static_vector<
    uint32_t,
    traits_type::write_set_expected_size> write_set_u32_vec_static;

  // use static types if the expected sizes are guarantees
  // For TPCC hard_expected_sizes is always true
  typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      read_set_map_static, read_set_map_small>::type read_set_map;
  typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      read_access_entry_static, read_access_entry_map_small>::type read_access_entry_map;
  typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      write_set_map_static, write_set_map_small>::type write_set_map;
  typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      write_access_entry_static, write_access_entry_map_small>::type write_access_entry_map;
  typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      absent_set_map_static, absent_set_map_small>::type absent_set_map;
  typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      write_set_u32_vec_static, write_set_u32_vec_small>::type write_set_u32_vec;
  typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      dep_queue_map_static, dep_queue_map_small>::type dep_queue_map;
  typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      read_unexposed_map_static, read_unexposed_map_small>::type read_unexposed_map;
  typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      write_unexposed_map_static, write_unexposed_map_small>::type write_unexposed_map;


  typedef std::vector<struct insert_record_t> insert_set_map;

  //For simple
  typedef std::vector<struct commutative_record_t> commutative_map;

#else
  typedef std::vector<read_record_t> read_set_map;
  typedef std::vector<write_record_t> write_set_map;
  typedef std::vector<absent_record_t> absent_set_map;
  typedef std::vector<uint32_t> write_set_u32_vec;
#endif

  template <typename T>
    using write_set_sized_vec =
      typename std::conditional<
        traits_type::hard_expected_sizes,
        static_vector<T, traits_type::write_set_expected_size>,
        typename util::vec<T, traits_type::write_set_expected_size>::type
      >::type;

  // small type
  typedef
    typename util::vec<
      dbtuple_read_write_info, traits_type::read_set_expected_size + traits_type::write_set_expected_size>::type
    dbtuple_read_write_info_vec_small;

  typedef
    typename util::vec<
      dbtuple_write_info, traits_type::write_set_expected_size>::type
    dbtuple_write_info_vec_small;

  typedef
    typename util::vec<
      dbtuple_read_info, traits_type::read_set_expected_size>::type
    dbtuple_read_info_vec_small;

  // static type
  typedef
    static_vector<
      dbtuple_read_write_info, traits_type::read_set_expected_size + traits_type::write_set_expected_size>
    dbtuple_read_write_info_vec_static;

  typedef
    static_vector<
      dbtuple_write_info, traits_type::write_set_expected_size>
    dbtuple_write_info_vec_static;

  typedef
    static_vector<
      dbtuple_read_info, traits_type::read_set_expected_size>
    dbtuple_read_info_vec_static;

  // chosen type
  typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      dbtuple_read_write_info_vec_static, dbtuple_read_write_info_vec_small>::type
    dbtuple_read_write_info_vec;

  typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      dbtuple_write_info_vec_static, dbtuple_write_info_vec_small>::type
    dbtuple_write_info_vec;

    typedef
    typename std::conditional<
      traits_type::hard_expected_sizes,
      dbtuple_read_info_vec_static, dbtuple_read_info_vec_small>::type
    dbtuple_read_info_vec;

  static inline bool
  sorted_dbtuples_contains(
      const dbtuple_write_info_vec &dbtuples,
      const dbtuple *tuple)
  {
    // XXX: skip binary search for small-sized dbtuples?
#ifdef USE_MCS_LOCK
    // the dbtuples are sorted in tuple's mcs_lock address, so here the binary search should also be comparing lock addr
    return std::binary_search(
        dbtuples.begin(), dbtuples.end(),
        dbtuple_write_info(tuple),
        [](const dbtuple_write_info &lhs, const dbtuple_write_info &rhs)
          { return lhs.get_tuple()->m_lock < rhs.get_tuple()->m_lock; });
#else
    return std::binary_search(
        dbtuples.begin(), dbtuples.end(),
        dbtuple_write_info(tuple),
        [](const dbtuple_write_info &lhs, const dbtuple_write_info &rhs)
          { return lhs.get_tuple() < rhs.get_tuple(); });
#endif
  }

public:

  inline transaction(uint64_t flags, string_allocator_type &sa);
  inline ~transaction();

  PolicyAction* getPA(uint32_t acc_id, uint16_t record_contention = 0, OP_TYPE op_type = OP_TYPE::OP_RW)
  {
    if (pg == nullptr)
    {
      return nullptr;
    }
    return pg->inference(acc_id, record_contention, op_type);
  }

  inline void
  init(conflict_graph* cg, uint8_t t, Policy *pol = nullptr, PolicyGradient *p = nullptr)
  {
    c_graph = cg;
    txn_type = t;
    policy = pol;
    pg = p;
    mix_op.set_policy(pol);
    mix_op.set_pg(p);
    ALWAYS_ASSERT(t);
    finish_init();
    // fprintf(stderr, "txn init finish t[%d] tid [%lx]\n", txn_type, tid);
  }

  // returns TRUE on successful commit, FALSE on abort
  // if doThrow`2, signals success by returning true, and
  // failure by throwing an abort exception
  bool commit(bool doThrow = false);

  // abort() always succeeds
  inline void
  abort()
  {
    abort_impl(ABORT_REASON_USER);
  }

  void dump_debug_info() const;

#ifdef DIE_ON_ABORT
  void
  abort_trap(abort_reason reason)
  {
    AbortReasonCounter(reason)->inc();
    this->reason = reason; // for dump_debug_info() to see
    dump_debug_info();
    ::abort();
  }
#else
  inline ALWAYS_INLINE void
  abort_trap(abort_reason reason)
  {
    AbortReasonCounter(reason)->inc();
  }
#endif

  std::map<std::string, uint64_t> get_txn_counters() const;

  inline ALWAYS_INLINE bool
  is_snapshot() const
  {
    return get_flags() & TXN_FLAG_READ_ONLY;
  }

  // for debugging purposes only
  inline const read_set_map &
  get_read_set() const
  {
    return read_set;
  }

  inline const write_set_map &
  get_write_set() const
  {
    return write_set;
  }

  inline const absent_set_map &
  get_absent_set() const
  {
    return absent_set;
  }

  uint8_t
  get_type()
  {
    return txn_type;
  }

  uint32_t
  get_cstep()
  {
    return (uint32_t)(cur_step >> CSTEP_SHIFT);
  }

  uint64_t
  get_step()
  {
    return cur_step;
  }

  uint64_t
  to_step(uint32_t pnum, uint8_t state)
  {
    INVARIANT(state < 4);
    return (((uint64_t)pnum) << CSTEP_SHIFT | state);
  }

  void 
  update_piece_state(uint8_t state)
  {
    INVARIANT(state >= 1 && state <= 3);

    uint64_t step = cur_step >> CSTEP_SHIFT;
    step = (step << CSTEP_SHIFT) | state;
    cur_step = step;

  }

  void
  update_step()
  {
  
    INVARIANT(cur_step == 0 || (uint8_t)cur_step == 3);

    uint64_t step = cur_step >> CSTEP_SHIFT;
    step++;
    cur_step = (step << CSTEP_SHIFT);
  }

  void
  set_step(uint32_t v)
  {
  
    INVARIANT(cur_step == 0 || (uint8_t)cur_step == 3);

    uint64_t step = v;
    cur_step = (step << CSTEP_SHIFT);
  }

  bool 
  wait_step(uint64_t wstep)
  {
    return cur_step < wstep;
  }

  bool
  finish_step(uint64_t wstep)
  {
      return cur_step > wstep;
  }

  bool is_finish(uint64_t t)
  {
    return (t != tid 
            || (state != TXN_ACTIVE && state != TXN_EMBRYO));
  }

  bool is_commit(uint64_t t)
  {
    return (t != tid || state == TXN_COMMITED);
  }

  bool is_abort(uint64_t t)
  {
    return (t != tid || state == TXN_ABRT);
  }

  //Acquire all tuples' locks in read set since rs_start
  void acquire_rs_locks(uint32_t rs_start);

  //Release all tuples' locks in read set since rs_start
  void release_rs_locks(uint32_t rs_start);


  uint32_t get_write_number()
  {
    return write_set.size();
  }

protected:
  inline void abort_impl(abort_reason r);

  inline void chamcc_abort_impl(dbtuple_write_info_vec *write_dbtuples, bool lock_mode = true);

  // assumes lock on marker is held on marker by caller, and marker is the
  // latest: removes marker from tree, and clears latest
  void cleanup_inserted_tuple_marker(
      dbtuple *marker, const std::string &key,
      concurrent_btree *btr);

  // low-level API for txn_btree

  // try to insert a new "tentative" tuple into the underlying
  // btree associated with the given context.
  //
  // if return.first is not null, then this function will
  //   1) mutate the transaction such that the absent_set is aware of any
  //      mutating changes made to the underlying btree.
  //   2) add the new tuple to the write_set
  //
  // if return.second is true, then this txn should abort, because a conflict
  // was detected w/ the absent_set.
  //
  // if return.first is not null, the returned tuple is locked()!
  //
  // if the return.first is null, then this function has no side effects.
  //
  // NOTE: !ret.first => !ret.second
  // NOTE: assumes key/value are stable
  std::pair< dbtuple *, bool >
  try_insert_new_tuple(
      concurrent_btree &btr,
      const std::string *key,
      const void *value,
      dbtuple::tuple_writer_t writer);

  // reads the contents of tuple into v
  // within this transaction context
  template <typename ValueReader>
  bool
   do_tuple_read(const dbtuple *tuple, ValueReader &value_reader, bool occ = true);

  void
  do_node_read(const typename concurrent_btree::node_opaque_t *n, 
    uint64_t version, const std::string* lkey, const std::string* ukey, bool occ = true);


protected:


  bool rendezvous(bool wait);

  bool serialization();

  bool check_piece_state(void *txn, uint64_t tid, uint32_t piece, uint8_t state);

  bool piece_sync();


public:
  // expected public overrides

  /**
   * Can we overwrite prev with cur?
   */
  bool can_overwrite_record_tid(tid_t prev, tid_t cur) const;

  inline string_allocator_type &
  string_allocator()
  {
    return *sa;
  }


  template <typename ValueReader> 
  bool 
  do_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader, uint32_t acc_id);

  template <typename ValueReader> 
  bool 
  profile_do_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader, ic3_profile* prof);



  template <typename ValueReader> 
  bool 
  do_snapshot_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader);

  template <typename ValueWriter> 
  bool 
  do_put(concurrent_btree &btr, const std::string* key_str, void* val_str, 
    ValueWriter& value_writer, bool expect_new, uint32_t acc_id);

  template <typename ValueReader, typename ValueWriter>
  bool
  do_update(concurrent_btree &btr, const std::string *key_str, ValueReader &value_reader, update_callback *callback,
            ValueWriter &value_writer, uint32_t acc_id);

  inline void before_access_operation(uint32_t acc_id, uint16_t contention, PolicyAction *pa = nullptr)
  {
    // if (unlikely(!policy || acc_id == MAX_ACC_ID))
    //   return;

    // // update txn progress
    // // todo - check the overhead here
    if (pa == nullptr)
    {
      return;
    }
    if (unlikely(ongoing_acc_id == MAX_ACC_ID)) {
      txn_first_acc_id = acc_id;
      ongoing_acc_id = acc_id;
      set_step(ongoing_acc_id - txn_first_acc_id + 1);
    } else if (acc_id > ongoing_acc_id) {
      ongoing_acc_id = acc_id;
      set_step(ongoing_acc_id - txn_first_acc_id + 1);
    }

    // waiting
    // do_wait(policy->inference_wait(acc_id, contention), policy->inference_wait_access(txn_type, contention));
    do_wait(pa->GetWaitAction(), pa->GetAccessWaitInfo());
  }

  inline void before_commit_piece_operation(uint32_t acc_id, uint16_t contention, PolicyAction *pa = nullptr)
  {
    // ALWAYS_ASSERT(policy && acc_id != MAX_ACC_ID);

    // // unlike before_access_operation(), no need to update step here

    // // waiting
    // do_wait(policy->inference_wait(acc_id, contention), policy->inference_wait_access(txn_type, contention));
    if(pa == nullptr)
    {
      return;
    }
    do_wait(pa->GetValidationWaitAction(), pa->GetValidationWaitInfo());
  }

  inline bool inference_scan_access(uint32_t acc_id, uint16_t contention, PolicyAction *pa = nullptr)
  {
    // if (unlikely(policy == nullptr || acc_id == MAX_ACC_ID)) {
    //   // default occ
    //   return true;
    // }
    // return policy->inference_access(acc_id, 0 /* todo - scan contention set to be 0 for now */);
    if (pa == nullptr)
    {
      return false;
    }
    return pa->GetAccessAction();
  }

  inline bool inference_commit_piece(uint32_t acc_id, uint16_t contention, PolicyAction *pa = nullptr) {
    // return true to do the piece commit
    // return policy->inference_piece_end(acc_id, contention);
    if (pa == nullptr)
    {
      return false;
    }
    return pa->GetPieceEndDecision();
  }

  // uint16_t get_unexposed_set_contention();

  // bool for whether piece successfully commits
  // uint32_t for where to "goto retry label"
  std::pair<bool, uint32_t> expose_uncommitted(uint32_t acc_id);

  tid_t
  get_tid() {
    return tid;
  }

  tid_t
  get_pid() 
  {
    unsigned numId = cast()->NumId(tid);
    unsigned coreId = cast()->CoreId(tid);
    unsigned pId = ++pnum;
    return cast()->MakeTid(coreId, numId, pId);
  }

  tid_t
  get_commit_tid() {
    return commit_tid;
  }

  void
  set_commit_tid(tid_t id) {
    commit_tid = id;
  }

  void
  check_abort();

  bool
  should_abort();

  void
  atomic_piece_begin(bool one_tuple);

  bool
  atomic_piece_end();

  bool
  atomic_piece_abort();

  // execute the waiting logic according to the agent decision
  void do_wait(AgentDecision decision, uint32_t* wait_access_info);

protected:
  // expected protected overrides

  /**
   * create a new, unique TID for a txn. at the point which gen_commit_tid(),
   * it still has not been decided whether or not this txn will commit
   * successfully
   */
  tid_t gen_commit_tid(const dbtuple_write_info_vec &write_tuples);

  bool can_read_tid(tid_t t) const;

  // For GC handlers- note that on_dbtuple_spill() is called
  // with the lock on ln held, to simplify GC code
  //
  // Is also called within an RCU read region
  void on_dbtuple_spill(dbtuple *tuple_ahead, dbtuple *tuple);

  // Called when the latest value written to ln is an empty
  // (delete) marker. The protocol can then decide how to schedule
  // the logical node for actual deletion
  void on_logical_delete(dbtuple *tuple, const std::string &key, concurrent_btree *btr);

  // if gen_commit_tid() is called, then on_tid_finish() will be called
  // with the commit tid. before on_tid_finish() is called, state is updated
  // with the resolution (commited, aborted) of this txn
  void on_tid_finish(tid_t commit_tid);

  void on_post_rcu_region_completion();


public:

  inline access_entry* insert_read_set(dbtuple* tuple, dbtuple::tid_t cur_pid, uint64_t pid, uint64_t tid, void *txn, bool occ = true, uint16_t record_contention = 0, uint64_t clean_tid = dbtuple::MIN_TID)
  {
    // read_access_map.emplace_back(pid, tid, txn);
    // read_set.emplace_back(tuple, cur_pid, pid, tid, txn, clean_tid, occ, record_contention, &read_access_map.back());
    read_set.emplace_back(tuple, cur_pid, pid, tid, txn, clean_tid, occ, record_contention, nullptr);

    if (!occ && record_contention > piece_contention) piece_contention = record_contention;

    if (record_contention > rw_highest_contention) rw_highest_contention = record_contention;

    return nullptr;
    // return read_set.back().get_access_entry();
  }

  inline access_entry* insert_read_access_entry(uint64_t pid, uint64_t tid, void *txn)
  {
    read_access_map.emplace_back(pid, tid, txn);
    return &read_access_map.back();
  }

  inline access_entry* insert_write_set(dbtuple* tuple,
                                        const std::string* key, const void *value, dbtuple::tuple_writer_t w,
                                        concurrent_btree *btr, bool insert, access_entry* e,
                                        void *txn = nullptr, bool occ = true, uint16_t record_contention = 0,
                                        uint64_t tid = dbtuple::MIN_TID, uint64_t pid = dbtuple::MIN_TID, 
                                        concurrent_btree::node_opaque_t *node = nullptr)
  {
    // write_access_map.emplace_back(pid, tid, txn);
    // write_set.emplace_back(tuple, key, value, w, btr, insert, e, pid, tid, txn, occ, record_contention, &write_access_map.back());
    write_set.emplace_back(tuple, key, node, value, w, btr, insert, e, pid, tid, txn, occ, record_contention, nullptr);

    if (record_contention > piece_contention) piece_contention = record_contention;
    
    if (record_contention > rw_highest_contention) rw_highest_contention = record_contention;

    return nullptr;
    // return write_set.back().get_access_entry();
  }

  inline access_entry* insert_write_access_entry(uint64_t pid, uint64_t tid, void *txn)
  {
    write_access_map.emplace_back(pid, tid, txn);
    return &write_access_map.back();
  }

  // inline void insert_unexposed_read(int pos) {
  //   read_unexposed.emplace_back(pos);
  // }

  // inline void insert_unexposed_write(int pos) {
  //   write_unexposed.emplace_back(pos);
  // }

  // inline void clear_unexposed_operations() {
  //   read_unexposed.clear();
  //   write_unexposed.clear();
  // }

  // inline void put_insert_set(concurrent_btree::node_opaque_t *n, const std::string* key)
  // {
  //   typename insert_set_map::iterator it = insert_set.begin();

  //   for (; it != insert_set.end(); ++it) {
  //     if(it->node == n && *it->key == *key)
  //       return;
  //   }

  //   insert_set.emplace_back(insert_record_t(n, key));
    
  // }

  inline void add_dep_txn(uint64_t tid, void* txn, bool dirty_read_dep = false);


public:
  inline void clear();
  inline void safe_gc();
  // SLOW accessor methods- used for invariant checking

  typename read_set_map::iterator
  find_read_set(const dbtuple *tuple)
  {
    // linear scan- returns the *first* entry found
    // (a tuple can exist in the read_set more than once)
    typename read_set_map::iterator it     = read_set.begin();
    typename read_set_map::iterator it_end = read_set.end();
    for (; it != it_end; ++it)
      if (it->get_tuple() == tuple)
        break;
    return it;
  }

  inline typename read_set_map::const_iterator
  find_read_set(const dbtuple *tuple) const
  {
    return const_cast<transaction *>(this)->find_read_set(tuple);
  }

  typename write_set_map::iterator
  find_write_set(dbtuple *tuple)
  {
    // linear scan- returns the *first* entry found
    // (a tuple can exist in the write_set more than once)
    typename write_set_map::iterator it     = write_set.begin();
    typename write_set_map::iterator it_end = write_set.end();
    for (; it != it_end; ++it)
      if (it->get_tuple() == tuple)
        break;
    return it;
  }

  inline typename write_set_map::const_iterator
  find_write_set(const dbtuple *tuple) const
  {
    return const_cast<transaction *>(this)->find_write_set(tuple);
  }

  inline bool
  handle_last_tuple_in_group(
      dbtuple_write_info &info, bool did_group_insert, bool lock_mode);

  inline bool
  handle_last_tuple_in_group_expose_piece(
      dbtuple_read_write_info &info, bool is_write, bool lock_mode);


  volatile uint32_t txn_type;
  volatile uint64_t cur_step;
  volatile tid_t tid;
  volatile tid_t commit_tid;

  string_allocator_type *sa;
  unsigned pnum;
  
  conflict_graph* c_graph;
  unmanaged<scoped_rcu_region> rcu_guard_;
//  op_wrapper<transaction<Protocol, Traits>> piece;
  mix_op<transaction<Protocol, Traits>> mix_op;
  

  read_set_map read_set;
  read_access_entry_map read_access_map;
  write_set_map write_set;
  write_access_entry_map write_access_map;
  absent_set_map absent_set;
  // insert_set_map insert_set;

  dep_queue_map  dep_queue;
  // commutative_map commutative_set;

  // read_unexposed_map read_unexposed;
  // write_unexposed_map write_unexposed;

  uint32_t read_set_start;
  uint32_t read_access_start;
  uint32_t write_set_start;
  uint32_t write_access_start;
  // uint32_t insert_set_start;
  uint32_t dep_queue_start;
  absent_set_map valid_absent_set;

protected:
  Policy* policy;
  PolicyGradient* pg;
  uint32_t txn_first_acc_id;
  uint32_t ongoing_acc_id;
  uint32_t valid_acc_id;
  uint32_t last_commit_piece_acc_id;

  uint16_t rw_highest_contention; // the highest contention this txn has ever seen
  uint16_t piece_contention;
  std::vector<void *> clean_read_failed_tuples;

public:

  inline uint32_t get_retry_point() {
    if (last_commit_piece_acc_id == MAX_ACC_ID) return txn_first_acc_id;
    else return (last_commit_piece_acc_id - ACCESSES + 1);
  }

  inline uint16_t get_txn_contention() {
    return rw_highest_contention;
  }

  inline uint16_t get_piece_contention() {
    return piece_contention;
  }

  inline bool add_clean_read_failed_tuples(void *tuple) {
    // return whether it is a new failed tuple
    if(find(clean_read_failed_tuples.begin(), clean_read_failed_tuples.end(), tuple) == clean_read_failed_tuples.end()) {
      clean_read_failed_tuples.emplace_back(tuple);
      return true;
    }
    return false;
  }

  inline void set_clean_read_failed_tuples(std::vector<void *>& tuples) {
    for (auto it : tuples)
	    add_clean_read_failed_tuples(it);
  }

  inline std::vector<void *> *get_clean_read_failed_tuples() {
    return &clean_read_failed_tuples;
  }
};

class transaction_abort_exception : public std::exception {
public:
  transaction_abort_exception(transaction_base::abort_reason r)
    : r(r) {}
  inline transaction_base::abort_reason
  get_reason() const
  {
    return r;
  }
  virtual const char *
  what() const throw()
  {
    return transaction_base::AbortReasonStr(r);
  }
private:
  transaction_base::abort_reason r;
};

// XXX(stephentu): stupid hacks
// XXX(stephentu): txn_epoch_sync is a misnomer
template <template <typename> class Transaction>
struct txn_epoch_sync {
  // block until the next epoch
  static inline void sync() {}
  // finish any async jobs
  static inline void finish() {}
  // run this code when a benchmark worker finishes
  static inline void thread_end() {}
  // how many txns have we persisted in total, from
  // the last reset invocation?
  static inline std::pair<uint64_t, double>
    compute_ntxn_persisted() { return {0, 0.0}; }
  // reset the persisted counters
  static inline void reset_ntxn_persisted() {}
};

#endif /* _NDB_TXN_H_ */
