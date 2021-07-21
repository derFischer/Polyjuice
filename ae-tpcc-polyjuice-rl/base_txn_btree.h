#ifndef _NDB_BASE_TXN_BTREE_H_
#define _NDB_BASE_TXN_BTREE_H_

#include "btree_choice.h"
#include "txn.h"
#include "lockguard.h"
#include "util.h"
#include "ndb_type_traits.h"

#include <string>
#include <map>
#include <type_traits>
#include <memory>

// each Transaction implementation should specialize this for special
// behavior- the default implementation is just nops
template <template <typename> class Transaction>
struct base_txn_btree_handler {
  static inline void on_construct() {} // called when initializing
  static const bool has_background_task = false;
};

template <template <typename> class Transaction, typename P>
class base_txn_btree {
public:

  typedef transaction_base::tid_t tid_t;
  typedef transaction_base::size_type size_type;
  typedef transaction_base::string_type string_type;
  typedef concurrent_btree::string_type keystring_type;

  base_txn_btree(size_type value_size_hint = 128,
            bool mostly_append = false,
            const std::string &name = "<unknown>")
    : value_size_hint(value_size_hint),
      name(name),
      been_destructed(false)
  {
    base_txn_btree_handler<Transaction>::on_construct();
  }

  ~base_txn_btree()
  {
    if (!been_destructed)
      unsafe_purge(false);
  }

  inline size_t
  size_estimate() const
  {
    return underlying_btree.size();
  }

  inline size_type
  get_value_size_hint() const
  {
    return value_size_hint;
  }

  inline void
  set_value_size_hint(size_type value_size_hint)
  {
    this->value_size_hint = value_size_hint;
  }

  inline void print() {
    underlying_btree.print();
  }

  /**
   * only call when you are sure there are no concurrent modifications on the
   * tree. is neither threadsafe nor transactional
   *
   * Note that when you call unsafe_purge(), this txn_btree becomes
   * completely invalidated and un-usable. Any further operations
   * (other than calling the destructor) are undefined
   */
  std::map<std::string, uint64_t> unsafe_purge(bool dump_stats = false);

private:

  struct purge_tree_walker : public concurrent_btree::tree_walk_callback {
    virtual void on_node_begin(const typename concurrent_btree::node_opaque_t *n);
    virtual void on_node_success();
    virtual void on_node_failure();
#ifdef TXN_BTREE_DUMP_PURGE_STATS
    purge_tree_walker()
      : purge_stats_nodes(0),
        purge_stats_nosuffix_nodes(0) {}
    std::map<size_t, size_t> purge_stats_tuple_record_size_counts; // just the record
    std::map<size_t, size_t> purge_stats_tuple_alloc_size_counts; // includes overhead
    //std::map<size_t, size_t> purge_stats_tuple_chain_counts;
    std::vector<uint16_t> purge_stats_nkeys_node;
    size_t purge_stats_nodes;
    size_t purge_stats_nosuffix_nodes;

    std::map<std::string, uint64_t>
    dump_stats()
    {
      std::map<std::string, uint64_t> ret;
      size_t v = 0;
      for (std::vector<uint16_t>::iterator it = purge_stats_nkeys_node.begin();
          it != purge_stats_nkeys_node.end(); ++it)
        v += *it;
      const double avg_nkeys_node = double(v)/double(purge_stats_nkeys_node.size());
      const double avg_fill_factor = avg_nkeys_node/double(concurrent_btree::NKeysPerNode);
      std::cerr << "btree node stats" << std::endl;
      std::cerr << "    avg_nkeys_node: " << avg_nkeys_node << std::endl;
      std::cerr << "    avg_fill_factor: " << avg_fill_factor << std::endl;
      std::cerr << "    num_nodes: " << purge_stats_nodes << std::endl;
      std::cerr << "    num_nosuffix_nodes: " << purge_stats_nosuffix_nodes << std::endl;
      std::cerr << "record size stats (nbytes => count)" << std::endl;
      for (std::map<size_t, size_t>::iterator it = purge_stats_tuple_record_size_counts.begin();
          it != purge_stats_tuple_record_size_counts.end(); ++it)
        std::cerr << "    " << it->first << " => " << it->second << std::endl;
      std::cerr << "alloc size stats  (nbytes => count)" << std::endl;
      for (std::map<size_t, size_t>::iterator it = purge_stats_tuple_alloc_size_counts.begin();
          it != purge_stats_tuple_alloc_size_counts.end(); ++it)
        std::cerr << "    " << (it->first + sizeof(dbtuple)) << " => " << it->second << std::endl;
      //std::cerr << "chain stats  (length => count)" << std::endl;
      //for (std::map<size_t, size_t>::iterator it = purge_stats_tuple_chain_counts.begin();
      //    it != purge_stats_tuple_chain_counts.end(); ++it) {
      //  std::cerr << "    " << it->first << " => " << it->second << std::endl;
      //  ret["chain_" + std::to_string(it->first)] += it->second;
      //}
      //std::cerr << "deleted recored stats" << std::endl;
      //std::cerr << "    logically_removed (total): " << (purge_stats_tuple_logically_removed_no_mark + purge_stats_tuple_logically_removed_with_mark) << std::endl;
      //std::cerr << "    logically_removed_no_mark: " << purge_stats_tuple_logically_removed_no_mark << std::endl;
      //std::cerr << "    logically_removed_with_mark: " << purge_stats_tuple_logically_removed_with_mark << std::endl;
      return ret;
    }
#endif

  private:
    std::vector< std::pair<typename concurrent_btree::value_type, bool> > spec_values;
  };

protected:

  // readers are placed here so they can be shared amongst
  // derived implementations

  template <typename Traits, typename Callback,
            typename KeyReader, typename ValueReader>
  struct txn_search_range_callback : public concurrent_btree::low_level_search_range_callback {
    constexpr txn_search_range_callback(
          Transaction<Traits> *t,
          Callback *caller_callback,
          KeyReader *key_reader,
          ValueReader *value_reader)
      : t(t), caller_callback(caller_callback),
        key_reader(key_reader), value_reader(value_reader) {}

    virtual void on_resp_node(const typename concurrent_btree::node_opaque_t *n, uint64_t version);
    virtual bool invoke(const typename concurrent_btree::string_type &k, typename concurrent_btree::value_type v,
                        const typename concurrent_btree::node_opaque_t *n, uint64_t version);

    void set_range(const std::string* lowk, const std::string* upk)
    {
      low_key = lowk;
      up_key = upk;
      INVARIANT(!up_key || !low_key || *low_key <= *up_key);
    }

    void set_occ()
    {
      occ = true;
    }

  private:
    Transaction<Traits> *const t;
    Callback *const caller_callback;
    KeyReader *const key_reader;
    ValueReader *const value_reader;
    const std::string* low_key;
    const std::string* up_key;
    bool occ = false;
  };

  template <typename Traits, typename ValueReader>
  inline bool
  do_search(Transaction<Traits> &t,
            const typename P::Key &k,
            ValueReader &value_reader,
            uint32_t acc_id = MAX_ACC_ID);

  template <typename Traits, typename ValueReader>
  inline bool
  profile_do_search(Transaction<Traits> &t,
            const typename P::Key &k,
            ValueReader &value_reader,
            ic3_profile* prof);

  template <typename Traits, typename Callback,
            typename KeyReader, typename ValueReader>
  inline void
  do_search_range_call(Transaction<Traits> &t,
                       const typename P::Key &lower,
                       const typename P::Key *upper,
                       Callback &callback,
                       KeyReader &key_reader,
                       ValueReader &value_reader,
                       uint32_t acc_id = MAX_ACC_ID);

  template <typename Traits, typename Callback,
            typename KeyReader, typename ValueReader>
  inline void
  do_rsearch_range_call(Transaction<Traits> &t,
                        const typename P::Key &upper,
                        const typename P::Key *lower,
                        Callback &callback,
                        KeyReader &key_reader,
                        ValueReader &value_reader,
                        uint32_t acc_id = MAX_ACC_ID);

  // expect_new indicates if we expect the record to not exist in the tree-
  // is just a hint that affects perf, not correctness. remove is put with nullptr
  // as value.
  //
  // NOTE: both key and value are expected to be stable values already
  template <typename Traits>
  void do_tree_put(Transaction<Traits> &t,
                   const std::string *k,
                   const typename P::Value *v,
                   dbtuple::tuple_writer_t writer,
                   bool expect_new,
                   uint32_t acc_id = MAX_ACC_ID);

  template <typename Traits, typename ValueReader>
  void do_tree_update(Transaction<Traits> &t,
                      const std::string *k,
                      ValueReader &value_reader,
                      update_callback *callback,
                      dbtuple::tuple_writer_t writer,
                      uint32_t acc_id = MAX_ACC_ID);


  template <typename Traits>
  void do_commutative_act(Transaction<Traits> &t,
                          const std::string *key,
                          dbtuple::tuple_commutative_act act,
                          uint64_t param,
                          dbtuple::tuple_writer_t writer);

  concurrent_btree underlying_btree;
  size_type value_size_hint;
  std::string name;
  bool been_destructed;
};

namespace private_ {
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, txn_btree_search_probe0, txn_btree_search_probe0_cg)
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, txn_btree_search_probe1, txn_btree_search_probe1_cg)
}

template <template <typename> class Transaction, typename P>
template <typename Traits, typename ValueReader>
bool
base_txn_btree<Transaction, P>::do_search(
    Transaction<Traits> &t,
    const typename P::Key &k,
    ValueReader &value_reader,
    uint32_t acc_id)
{
  t.ensure_active();

  typename P::KeyWriter key_writer(&k);
  const std::string * const key_str =
    key_writer.fully_materialize(true, t.string_allocator());

  return t.do_get(this->underlying_btree, key_str, value_reader, acc_id);
}


template <template <typename> class Transaction, typename P>
template <typename Traits, typename ValueReader>
bool
base_txn_btree<Transaction, P>::profile_do_search(
    Transaction<Traits> &t,
    const typename P::Key &k,
    ValueReader &value_reader,
            ic3_profile* prof)
{
  uint64_t beg = rdtsc();

  t.ensure_active();

  typename P::KeyWriter key_writer(&k);
  const std::string * const key_str =
    key_writer.fully_materialize(true, t.string_allocator());

  bool res = t.profile_do_get(this->underlying_btree,  key_str, value_reader, prof);

  prof->data[ic3_profile::level::LEVEL1] += rdtsc() - beg;

  return res;
}


template <template <typename> class Transaction, typename P>
std::map<std::string, uint64_t>
base_txn_btree<Transaction, P>::unsafe_purge(bool dump_stats)
{
  ALWAYS_ASSERT(!been_destructed);
  been_destructed = true;
  purge_tree_walker w;
  scoped_rcu_region guard;
  underlying_btree.tree_walk(w);
  underlying_btree.clear();
#ifdef TXN_BTREE_DUMP_PURGE_STATS
  if (!dump_stats)
    return std::map<std::string, uint64_t>();
  return w.dump_stats();
#else
  return std::map<std::string, uint64_t>();
#endif
}

template <template <typename> class Transaction, typename P>
void
base_txn_btree<Transaction, P>::purge_tree_walker::on_node_begin(const typename concurrent_btree::node_opaque_t *n)
{
  INVARIANT(spec_values.empty());
  spec_values = concurrent_btree::ExtractValues(n);
}

template <template <typename> class Transaction, typename P>
void
base_txn_btree<Transaction, P>::purge_tree_walker::on_node_success()
{
  for (size_t i = 0; i < spec_values.size(); i++) {
    dbtuple *tuple = (dbtuple *) spec_values[i].first;
    INVARIANT(tuple);
#ifdef TXN_BTREE_DUMP_PURGE_STATS
    // XXX(stephentu): should we also walk the chain?
    purge_stats_tuple_record_size_counts[tuple->is_deleting() ? 0 : tuple->size]++;
    purge_stats_tuple_alloc_size_counts[tuple->alloc_size]++;
    //purge_stats_tuple_chain_counts[tuple->chain_length()]++;
#endif
    if (base_txn_btree_handler<Transaction>::has_background_task) {
#ifdef CHECK_INVARIANTS
      lock_guard<dbtuple> l(tuple, false);
#endif
      if (!tuple->is_deleting()) {
        INVARIANT(tuple->is_latest());
        tuple->clear_latest();
        tuple->mark_deleting();
        dbtuple::release(tuple);
      } else {
        // enqueued already to background gc by the writer of the delete
      }
    } else {
      // XXX: this path is probably not right
      dbtuple::release_no_rcu(tuple);
    }
  }
#ifdef TXN_BTREE_DUMP_PURGE_STATS
  purge_stats_nkeys_node.push_back(spec_values.size());
  purge_stats_nodes++;
  for (size_t i = 0; i < spec_values.size(); i++)
    if (spec_values[i].second)
      goto done;
  purge_stats_nosuffix_nodes++;
done:
#endif
  spec_values.clear();
}

template <template <typename> class Transaction, typename P>
void
base_txn_btree<Transaction, P>::purge_tree_walker::on_node_failure()
{
  spec_values.clear();
}

template <template <typename> class Transaction, typename P>
template <typename Traits>
void base_txn_btree<Transaction, P>::do_tree_put(
    Transaction<Traits> &t,
    const std::string *k,
    const typename P::Value *v,
    dbtuple::tuple_writer_t writer,
    bool expect_new,
    uint32_t acc_id)
{
  INVARIANT(k);
  INVARIANT(!expect_new || v); // makes little sense to remove() a key you expect
                               // to not be present, so we assert this doesn't happen
                               // for now [since this would indicate a suboptimality]
  t.ensure_active();

  if (unlikely(t.is_snapshot())) {
    const transaction_base::abort_reason r = transaction_base::ABORT_REASON_USER;
    t.abort_impl(r);
    throw transaction_abort_exception(r);
  }

  bool res = t.do_put(this->underlying_btree, k, (void *)(v), writer, expect_new, acc_id);


  if(!res) {
    const transaction_base::abort_reason r = transaction_base::ABORT_REASON_INSERT_NODE_INTERFERENCE;
    t.abort_impl(r);
    throw transaction_abort_exception(r);
  }

}

template <template <typename> class Transaction, typename P>
template <typename Traits, typename ValueReader>
void base_txn_btree<Transaction, P>::do_tree_update(
    Transaction<Traits> &t,
    const std::string *k,
    ValueReader &value_reader,
    update_callback *callback,
    dbtuple::tuple_writer_t writer,
    uint32_t acc_id)
{
  INVARIANT(k);
  INVARIANT(!t.is_snapshot());
  t.ensure_active();

  bool res = t.do_update(this->underlying_btree, k, value_reader, callback, writer, acc_id);

  if(!res) {
    const transaction_base::abort_reason r = transaction_base::ABORT_REASON_INSERT_NODE_INTERFERENCE;
    t.abort_impl(r);
    throw transaction_abort_exception(r);
  }

}

template <template <typename> class Transaction, typename P>
template <typename Traits>
void base_txn_btree<Transaction, P>::do_commutative_act(Transaction<Traits> &t,
                                      const std::string *k,
                                      dbtuple::tuple_commutative_act act,
                                      uint64_t param,
                                      dbtuple::tuple_writer_t w)
{
  t.ensure_active();

  typename concurrent_btree::value_type bv = 0;
  ALWAYS_ASSERT(this->underlying_btree.search(varkey(*k), bv));

  dbtuple* tuple = reinterpret_cast<dbtuple *>(bv);

  ALWAYS_ASSERT(tuple);

  // t.commutative_set.emplace_back(tuple, k, act, param, &this->underlying_btree, w);

}

template <template <typename> class Transaction, typename P>
template <typename Traits, typename Callback,
          typename KeyReader, typename ValueReader>
void
base_txn_btree<Transaction, P>
  ::txn_search_range_callback<Traits, Callback, KeyReader, ValueReader>
  ::on_resp_node(
    const typename concurrent_btree::node_opaque_t *n, uint64_t version)
{
  VERBOSE(std::cerr << "on_resp_node(): <node=0x" << util::hexify(intptr_t(n))
               << ", version=" << version << ">" << std::endl);
  VERBOSE(std::cerr << "  " << concurrent_btree::NodeStringify(n) << std::endl);
  t->do_node_read(n, version, low_key, up_key, occ);
}


template <template <typename> class Transaction, typename P>
template <typename Traits, typename Callback,
          typename KeyReader, typename ValueReader>
bool
base_txn_btree<Transaction, P>
  ::txn_search_range_callback<Traits, Callback, KeyReader, ValueReader>
  ::invoke(
    const typename concurrent_btree::string_type &k, typename concurrent_btree::value_type v,
    const typename concurrent_btree::node_opaque_t *n, uint64_t version)
{
  t->ensure_active();
  VERBOSE(std::cerr << "search range k: " << util::hexify(k) << " from <node=0x" << util::hexify(n)
                    << ", version=" << version << ">" << std::endl
                    << "  " << *((dbtuple *) v) << std::endl);
  const dbtuple * const tuple = reinterpret_cast<const dbtuple *>(v);
  if (t->do_tuple_read(tuple, *value_reader, occ))
    return caller_callback->invoke(
        (*key_reader)(k), value_reader->results());
  return true;
}

template <template <typename> class Transaction, typename P>
template <typename Traits, typename Callback,
          typename KeyReader, typename ValueReader>
void
base_txn_btree<Transaction, P>::do_search_range_call(
    Transaction<Traits> &t,
    const typename P::Key &lower,
    const typename P::Key *upper,
    Callback &callback,
    KeyReader &key_reader,
    ValueReader &value_reader,
    uint32_t acc_id)
{
  t.ensure_active();
  PolicyAction* pa = t.getPA(acc_id);
  t.before_access_operation(acc_id, 0 /* todo - scan contention set to be 0 for now */, pa);
  bool occ = t.inference_scan_access(acc_id, 0 /* todo - scan contention set to be 0 for now */, pa);


  if (upper)
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                 << ")::search_range_call [" << util::hexify(lower)
                 << ", " << util::hexify(*upper) << ")" << std::endl);
  else
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                 << ")::search_range_call [" << util::hexify(lower)
                 << ", +inf)" << std::endl);

  typename P::KeyWriter lower_key_writer(&lower);
  const std::string * const lower_str =
    lower_key_writer.fully_materialize(true, t.string_allocator());

  typename P::KeyWriter upper_key_writer(upper);
  const std::string * const upper_str =
    upper_key_writer.fully_materialize(true, t.string_allocator());

  if (unlikely(upper_str && *upper_str <= *lower_str))
    return;

  txn_search_range_callback<Traits, Callback, KeyReader, ValueReader> c(
			&t, &callback, &key_reader, &value_reader);

  varkey uppervk;
  if (upper_str) 
    uppervk = varkey(*upper_str);

  if (occ)
  {
    c.set_occ();
  }
  else
  {
    c.set_range(lower_str, upper_str);
  }

  this->underlying_btree.search_range_call(
      varkey(*lower_str), upper_str ? &uppervk : nullptr,
      c/*, t.string_allocator()()*/);

}

template <template <typename> class Transaction, typename P>
template <typename Traits, typename Callback,
          typename KeyReader, typename ValueReader>
void
base_txn_btree<Transaction, P>::do_rsearch_range_call(
    Transaction<Traits> &t,
    const typename P::Key &upper,
    const typename P::Key *lower,
    Callback &callback,
    KeyReader &key_reader,
    ValueReader &value_reader,
    uint32_t acc_id)
{
  t.ensure_active();
  PolicyAction* pa = t.getPA(acc_id);
  t.before_access_operation(acc_id, 0 /* todo - scan contention set to be 0 for now */, pa);
  bool occ = t.inference_scan_access(acc_id, 0 /* todo - scan contention set to be 0 for now */, pa);

  typename P::KeyWriter lower_key_writer(lower);
  const std::string * const lower_str =
    lower_key_writer.fully_materialize(true, t.string_allocator());

  typename P::KeyWriter upper_key_writer(&upper);
  const std::string * const upper_str =
    upper_key_writer.fully_materialize(true, t.string_allocator());

  if (unlikely(lower_str && *upper_str <= *lower_str))
    return;

  txn_search_range_callback<Traits, Callback, KeyReader, ValueReader> c(
			&t, &callback, &key_reader, &value_reader);

  varkey lowervk;
  if (lower_str)
    lowervk = varkey(*lower_str);

  if (occ)
  {
    c.set_occ();
  }
  else
  {
    c.set_range(lower_str, upper_str);
  }

  this->underlying_btree.rsearch_range_call(
      varkey(*upper_str), lower_str ? &lowervk : nullptr,
      c/*, t.string_allocator()()*/);
}

#endif /* _NDB_BASE_TXN_BTREE_H_ */
