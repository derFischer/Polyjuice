 #ifndef _PIECE_IMPL_H_
#define _PIECE_IMPL_H_

#include "piece.h"
#include "tuple_impl.h"

//=================================MIX Operation================================

 template <typename Transaction>
 mix_op<Transaction>::mix_op(Transaction* t): txn(t)
 {}

 template <typename Transaction>
 mix_op<Transaction>::~mix_op() {}

 template <typename Transaction>
 void mix_op<Transaction>::set_policy(Policy *pol) {
   ALWAYS_ASSERT(pol);
   policy = pol;
 }

 template <typename Transaction>
 void mix_op<Transaction>::set_pg(PolicyGradient *pgg) {
   pg = pgg;
 }

 template <typename Transaction>
 void mix_op<Transaction>::begin_piece()
 {
   ALWAYS_ASSERT(false);
 }


 template <typename Transaction>
 bool mix_op<Transaction>::commit_piece()
 {
   ALWAYS_ASSERT(false);
   return true;
 }

 template <typename Transaction>
 void mix_op<Transaction>::abort_piece(bool b = false)
 {
   return;
 }

template <typename Transaction>
access_entry* mix_op<Transaction>::read_set_find(dbtuple* tuple)
{
  for(int i = 0; i < txn->read_set.size(); i++) {
    if(txn->read_set[i].get_tuple() == tuple) {
      return txn->read_set[i].get_access_entry();
    }
  }
  return nullptr;
}

template <typename Transaction>
template <typename ValueReader>
bool mix_op<Transaction>::do_get(
  concurrent_btree &btr,
  const std::string* key_str,
  ValueReader& value_reader,
  uint32_t acc_id)
{
  concurrent_btree::versioned_node_t search_info;
  typename concurrent_btree::value_type underlying_v{};

  if (!btr.search(varkey(*key_str), underlying_v, &search_info)){
    // both occ and ic3 action should add not found case to absent set
    do_node_read(search_info.first, search_info.second, key_str, key_str, true /* occ for not found case */);
    // todo - FIX: just return and skip before_access_operation()
    return false;
  }

  dbtuple *tuple = reinterpret_cast<dbtuple *>(underlying_v);

  uint16_t record_contention = 0;
  bool occ, lock_mode = true;

  // action inference w/ record contention knowledge
  // record_contention = tuple->get_counter();
  PolicyAction* pa = nullptr;
  if(pg)
  {
    pa = pg->inference(acc_id, record_contention, OP_TYPE::OP_RW);
  }
  if (unlikely(policy == nullptr || acc_id == MAX_ACC_ID || pg == nullptr || pa == nullptr)) {
    occ = true;
    lock_mode = true;
  } else {
    // occ = policy->inference_access(acc_id, record_contention);
    // lock_mode = policy->inference_lock_mode(acc_id, record_contention);
    occ = pa->GetAccessAction();
  }
  // before access 
  txn->before_access_operation(acc_id, record_contention, pa);

  if (occ) {
    bool ret = do_tuple_read(tuple, value_reader);
    // check whether read success (the tuple can be empty)
    if (!ret) do_node_read(search_info.first, search_info.second, key_str, key_str, true /* occ for not found case */);

    // if (ret) txn->insert_unexposed_read(txn->read_set.size() - 1);

    return ret;
  }

  // start dirty read action stuff
  tuple->prefetch();
  access_entry *e = txn->insert_read_set(tuple, dbtuple::MIN_TID, dbtuple::MIN_TID, txn->get_tid(), txn, occ, record_contention,
                                          tuple->version);
  tuple->chamcc_lock(lock_mode, nullptr);
  bool result = tuple->ic3_read(value_reader, txn->string_allocator(), nullptr);

  // update the clean tid of the tuple after mcs_lock on
  int index = txn->read_set.size() - 1;
  INVARIANT(index >= 0);
  txn->read_set[index].set_clean_tid(tuple->version);

  // set_dep_txn for dirty read
  access_entry *de = tuple->get_dependent_entry(false /*indicate read action*/);
  if (likely(de && !txn->read_set[index].is_occ())) {
    Transaction *d_txn = (Transaction *) de->get_txn();
    uint64_t d_tid = de->get_tid();

    if (d_tid != txn->get_tid() && d_txn != txn) {
      txn->read_set[index].set_dependent(de->get_txn(), d_tid, de->get_write_seq());
    }
  }

  // tuple->increase_counter();

  // txn->insert_unexposed_read(txn->read_set.size() - 1);

  tuple->chamcc_unlock(lock_mode, nullptr);

  return result;
}

 template <typename Transaction>
 template <typename ValueReader>
 bool mix_op<Transaction>::do_tuple_read(dbtuple* tuple, ValueReader& reader, bool occ, uint16_t record_contention)
 {
   if(tuple == nullptr)
     return false;

   tuple->prefetch();
   transaction_base::tid_t start_t = 0;
   const auto snapshot_tid = static_cast<transaction_base::tid_t>(dbtuple::MAX_TID);
   // only occ action can reach here, directly use the silo's read tuple interface
   dbtuple::ReadStatus stat = tuple->stable_read(snapshot_tid, start_t, reader, txn->string_allocator(), false);

   if (likely(stat == dbtuple::ReadStatus::READ_RECORD)) {
     txn->insert_read_set(tuple, dbtuple::MIN_TID, dbtuple::MIN_TID, txn->get_tid(), txn, occ /*occ*/, record_contention, start_t);
     return true;
   }
   return false;
 }

 template <typename Transaction>
 bool mix_op<Transaction>::do_node_read(const typename concurrent_btree::node_opaque_t *n,
                                                uint64_t v, const std::string* lkey, const std::string* ukey, bool occ)
 {
   auto it = txn->absent_set.find(n);

   if(it == txn->absent_set.end()) {
     //fprintf(stderr, "Try to put %lx [%d] \n", (uint64_t)n, txn->absent_set.size());
     txn->absent_set[n].version = v;
     txn->absent_set[n].low_key = lkey;
     txn->absent_set[n].up_key = ukey;
     txn->absent_set[n].occ = occ;
   } else if (it->second.version != v) {
    //  const piece_base::abort_reason r = piece_base::ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED;
    //  throw piece_abort_exception(r);
     const transaction_base::abort_reason r = transaction_base::ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED;
     throw transaction_abort_exception(r);
   }

   return true;
 }


template <typename Transaction>
template <typename ValueWriter>
bool mix_op<Transaction>::do_put(
  concurrent_btree &btr,
  const std::string* k,
  void* v,
  ValueWriter& writer,
  bool expect_new,
  uint32_t acc_id)
{
  dbtuple *px = nullptr;
  access_entry* entry = nullptr;

  uint16_t record_contention;
  bool occ, lock_mode = true;

retry:
  if (expect_new) {
    // action inference w/ record contention knowledge (contention 0 for insert)
    record_contention = 0;
    PolicyAction* pa = nullptr;
    if (pg)
    {
      pa = pg->inference(acc_id, record_contention, OP_TYPE::OP_RW);
    }
    if (unlikely(policy == nullptr || acc_id == MAX_ACC_ID || pg == nullptr || pa == nullptr)) {
      occ = true;
      lock_mode = true;
    } else {
      // occ = policy->inference_access(acc_id, record_contention);
      // lock_mode = policy->inference_lock_mode(acc_id, record_contention);
      occ = pa->GetAccessAction();
    }
    // before access
    txn->before_access_operation(acc_id, record_contention, pa);

    concurrent_btree::node_opaque_t *node;
    auto ret = try_insert_new_tuple(btr, k, v, writer, occ, &node);
    px = ret.first;

    if (px) {
      // successfully insert a new tuple, no need be in read set, not holding the lock of new inserted tuple
      ALWAYS_ASSERT(node);
      entry = txn->insert_write_set(px, k, v, writer, &btr, true, nullptr, txn, occ, record_contention, txn->get_tid(), dbtuple::MIN_TID, node);
      // txn->write_set[(txn->write_set.size() - 1)].set_inset_set_pos(txn->insert_set.size() - 1);
      // public-write action: store value in access entry
      // if (!occ) entry->write(v, dbtuple::MIN_TID, txn->string_allocator());

      // fix - put entry operation to expose_uncommitted
      // txn->insert_unexposed_write(txn->write_set.size() - 1);
      // if (!occ) entry->write(v, dbtuple::MIN_TID, txn->string_allocator());

      if (unlikely(ret.second)) {
        // const piece_base::abort_reason r = piece_base::ABORT_REASON_INSERT_FAILED;
        // throw piece_abort_exception(r);
        const transaction_base::abort_reason r = transaction_base::ABORT_REASON_INSERT_NODE_INTERFERENCE;
        throw transaction_abort_exception(r);
      }
    }
  }

  if (!px) {
    // do regular search
    typename concurrent_btree::value_type bv = 0;
    if (!btr.search(varkey(*k), bv)) {
      expect_new = true;
      goto retry;
    }

    px = reinterpret_cast<dbtuple *>(bv);

    // action inference w/ record contention knowledge
    // record_contention = px->get_counter();
    PolicyAction* pa = nullptr;
    if (pg)
    {
      pa = pg->inference(acc_id, record_contention, OP_TYPE::OP_RW);
    }
    if (unlikely(policy == nullptr || acc_id == MAX_ACC_ID || pg == nullptr || pa == nullptr)) {
      occ = true;
      lock_mode = true;
    } else {
      occ = pa->GetAccessAction();
      // occ = policy->inference_access(acc_id, record_contention);
      // lock_mode = policy->inference_lock_mode(acc_id, record_contention);
    }
    // before access - record already exist 
    txn->before_access_operation(acc_id, record_contention, pa);

    if (occ) {
      entry = txn->insert_write_set(px, k, v, writer, &btr, false, nullptr, txn, occ, record_contention, txn->get_tid());
      // fix - put entry operation to expose_uncommitted
      // txn->insert_unexposed_write(txn->write_set.size() - 1);
    } else {
      // set overwrite flag for related read operation for quick commit phase unlinking eccess  entry
      // fix - put entry operation to expose_uncommitted
      // access_entry *read_e = read_set_find(px);
      // if (read_e)
      //   read_e->set_overwrite();
      entry = txn->insert_write_set(px, k, v, writer, &btr, false, nullptr, txn, occ, record_contention, txn->get_tid());
      // fix - put entry operation to expose_uncommitted
      // txn->insert_unexposed_write(txn->write_set.size() - 1);
      // if (!v)
      //   entry->set_delete();
      // entry->write(v, dbtuple::MIN_TID, txn->string_allocator());
    }
  }

  if (occ)
    return true;

  // start dirty read action stuff
  INVARIANT(entry);

  // px->increase_counter();

  // fix - every write has to be insert_unexposed_write
  // txn->insert_unexposed_write(txn->write_set.size() - 1);

  return true;
 }


 template <typename Transaction>
 template <typename ValueWriter>
 std::pair< dbtuple *, bool >
 mix_op<Transaction>::try_insert_new_tuple(
     concurrent_btree &btr,
     const std::string* key,
     void* value,
     ValueWriter& writer,
     bool occ,
     concurrent_btree::node_opaque_t **node)
 {

   INVARIANT(value);
   const size_t sz =
       value ? writer(dbtuple::TUPLE_WRITER_COMPUTE_NEEDED,
                      value, nullptr, 0) : 0;

#ifdef USE_MCS_LOCK
   dbtuple * const tuple = dbtuple::alloc_first(sz, false);
#else
   //First alloc: version/subversion - MAXID
  dbtuple * const tuple = dbtuple::alloc_first(sz, false);
#endif
   //fprintf(stderr, "[atomic_mul_ops<Transaction>::try_insert_new_tuple] alloc tuple %lx\n", (uint64_t)tuple);

   if (value)
     writer(dbtuple::TUPLE_WRITER_DO_WRITE, value, tuple->get_value_start(), 0);

   typename concurrent_btree::insert_info_t insert_info;
   if (unlikely(!btr.insert_if_absent(
       varkey(*key), (typename concurrent_btree::value_type) tuple, &insert_info))) {
     VERBOSE(std::cerr << "insert_if_absent failed for key: " << util::hexify(key) << std::endl);

     tuple->clear_latest();

     //fprintf(stderr, "core[%d] insert_if_absent delete tuple %lx\n", coreid::core_id(), tuple );
     dbtuple::release_no_rcu(tuple);
     return std::pair< dbtuple *, bool >(nullptr, false);
   }

   //fprintf(stderr, "Insert Succ\n");
   if (!txn->absent_set.empty()) {
     //fprintf(stderr, "Try to find %lx\n", (uint64_t)insert_info.node);
     auto it = txn->absent_set.find(insert_info.node);
     if (it != txn->absent_set.end()) {
       if (unlikely(it->second.version != insert_info.old_version)) {
         return std::make_pair(tuple, true);
       }
       // otherwise, bump the version
       it->second.version = insert_info.new_version;
     }
   }

   (*node) = const_cast<concurrent_btree::node_opaque_t *>(insert_info.node);

    // txn->put_insert_set(const_cast<concurrent_btree::node_opaque_t *>(insert_info.node), key);

   return std::make_pair(tuple, false);
 }

template <typename Transaction>
template<typename ValueReader, typename ValueWriter>
bool mix_op<Transaction>::do_update(
  concurrent_btree &btr,
  const std::string *k,
  ValueReader &value_reader,
  update_callback *callback,
  ValueWriter &writer,
  uint32_t acc_id)
{
  concurrent_btree::versioned_node_t search_info;
  typename concurrent_btree::value_type underlying_v{};

  if (!btr.search(varkey(*k), underlying_v, &search_info)) {
    // both occ and ic3 action should add not found case to absent set
    do_node_read(search_info.first, search_info.second, k, k, true /* occ for not found case */);
    // benchmark should not reach here
    // if do reach here, check read operations in delivery transaction 
     // if do reach here, check read operations in delivery transaction 
    // if do reach here, check read operations in delivery transaction 
    ALWAYS_ASSERT(false);
    return false;
  }

  dbtuple *tuple = reinterpret_cast<dbtuple *>(underlying_v);

  uint16_t record_contention = 0;
  bool occ, lock_mode = true;

  // action inference w/ record contention knowledge
  // record_contention = tuple->get_counter();
  PolicyAction* pa = nullptr;
  if (pg)
  {
    pa = pg->inference(acc_id, record_contention, OP_TYPE::OP_RW);
  }
  if (unlikely(policy == nullptr || acc_id == MAX_ACC_ID || pg == nullptr || pa == nullptr)) {
    occ = true;
    lock_mode = true;
  } else {
    occ = pa->GetAccessAction();
    // occ = policy->inference_access(acc_id, record_contention);
    // lock_mode = policy->inference_lock_mode(acc_id, record_contention);
  }
  // before access 
  txn->before_access_operation(acc_id, record_contention, pa);

  if (occ) {
    // action: cleanRead & privateWrite
    bool res = do_tuple_read(tuple, value_reader);
    if (res) {
      void *v = callback->invoke();
      txn->insert_write_set(tuple, k, v, writer, &btr, false, nullptr, txn, occ, record_contention, txn->get_tid());
      // fix - put entry operation to expose_uncommitted
      // txn->insert_unexposed_write(txn->write_set.size() - 1);
    }

    return res;
  }

  // action: dirtyRead & publicWrite
  tuple->prefetch();
  access_entry *read_e = txn->insert_read_set(tuple, dbtuple::MIN_TID, dbtuple::MIN_TID, txn->get_tid(), txn, occ, 
                                              record_contention, tuple->version);
  // read_e->set_overwrite();

  tuple->chamcc_lock(lock_mode, nullptr);
  bool result = tuple->ic3_read(value_reader, txn->string_allocator(), nullptr);

  void *v = callback->invoke();
  access_entry *write_e = txn->insert_write_set(tuple, k, v, writer, &btr, false, nullptr, txn, occ, record_contention, txn->get_tid());

  // fix - put entry operation to expose_uncommitted
  // txn->insert_unexposed_write(txn->write_set.size() - 1);
  // if (!v)
  //   write_e->set_delete();
  // write_e->write(v, dbtuple::MIN_TID, txn->string_allocator());

  // update the clean tid of the tuple after mcs_lock on
  int index = txn->read_set.size() - 1;
  INVARIANT(index >= 0);
  txn->read_set[index].set_clean_tid(tuple->version);

  if (txn->read_set[index].get_dep_txn() == nullptr) {
    // maintain dirty read info for validation, get the latest write entry if there is any
    access_entry *read_de = tuple->get_dependent_entry(false /*indicate read action*/);
    if (likely(read_de && !txn->read_set[index].is_occ())) {
        // actually read a uncommitted value
        Transaction *d_txn = (Transaction *) read_de->get_txn();
        uint64_t d_tid = read_de->get_tid();

        if (d_tid != txn->get_tid() && d_txn != txn) {
          txn->read_set[index].set_dependent(read_de->get_txn(), d_tid, read_de->get_write_seq());
        }
    }
  }

  // tuple->increase_counter();

  // txn->insert_unexposed_read(txn->read_set.size() - 1);
  // fix - every write has to be insert_unexposed_write
  // txn->insert_unexposed_write(txn->write_set.size() - 1);
  
  tuple->chamcc_unlock(lock_mode, nullptr);

  return true;
}

template <typename Transaction>
void mix_op<Transaction>::check_rmw(int pos)
{
  // corresponding lock already hold here
  // this is a write operation
  access_entry *entry = nullptr;
  dbtuple *tuple = nullptr;
  entry = txn->write_set[pos].get_access_entry();
  tuple = txn->write_set[pos].get_tuple();
  ALWAYS_ASSERT(entry && tuple);

  access_entry *read_e = read_set_find(tuple);
  if (read_e)
    read_e->set_overwrite();
  txn->write_set[pos].set_read_access_entry(read_e);
}

template <typename Transaction>
bool mix_op<Transaction>::do_expose_ic3_action(int pos, bool is_write, bool lock_mode)
{
  // corresponding lock already hold here
  access_entry *entry = nullptr;
  dbtuple *tuple = nullptr;
  if (is_write) {
    entry = txn->write_set[pos].get_access_entry();
    tuple = txn->write_set[pos].get_tuple();
    ALWAYS_ASSERT(entry && tuple);
    ALWAYS_ASSERT(!entry->is_linked());
    ALWAYS_ASSERT(!entry->is_prepare());

    // do entry operations
    // access_entry *read_e = read_set_find(tuple);
    // if (read_e)
    //   read_e->set_overwrite();
    // txn->write_set[pos].set_read_access_entry(read_e);
    void* v = txn->write_set[pos].get_value();
    if (!v)
      entry->set_delete();
    entry->write(v, dbtuple::MIN_TID, txn->string_allocator());
    
  } else {
    entry = txn->read_set[pos].get_access_entry();
    tuple = txn->read_set[pos].get_tuple();
    if (!txn->read_set[pos].is_occ()) {
      ALWAYS_ASSERT(entry && tuple);
      ALWAYS_ASSERT(!entry->is_linked());
      ALWAYS_ASSERT(!entry->is_prepare());
    }
  }
  // ALWAYS_ASSERT(entry && tuple);
  // ALWAYS_ASSERT(!entry->is_linked());
  // ALWAYS_ASSERT(!entry->is_prepare());

  if (is_write) {
    access_entry *de = tuple->get_latest_dependent_entries();
    while (de) {
      Transaction *d_txn = (Transaction *) de->get_txn();
      uint64_t d_tid = de->get_tid();

      if (!d_txn->is_commit(d_tid)
          && d_tid != txn->get_tid()
          && d_txn != txn) {
        txn->add_dep_txn(de->get_tid(), de->get_txn());
      }
      de = de->next;
    }
  } else {
    if (txn->read_set[pos].is_occ()) {
      if (!tuple->is_latest_version(txn->read_set[pos].clean_tid()))
        return false;
      else
        return true;
    }
    
    access_entry *de = tuple->get_dependent_entry(entry->is_write());

    // begin: early valiation for read
    if (txn->read_set[pos].get_dep_txn() == nullptr) {
      // check things:
      // a.version unchanged
      // b.no write access entry exposed on access list
      if (!tuple->is_latest_version(txn->read_set[pos].clean_tid()) || !tuple->check_no_write_entry(txn->get_tid()))
        return false;
    } else {
      Transaction *read_dep_txn = (Transaction *) txn->read_set[pos].get_dep_txn();
      uint64_t read_dep_txn_tid = txn->read_set[pos].get_dep_tid();
      // check things:
      // if read_dep_txn committed: a.version unchanged compared to read_dep_txn's commitVersion b.no write access entry exposed on access list
      // if read_dep_txn ongoing: a.no write access entry exposed on access list after read_dep_txn's access entry
      if (read_dep_txn->is_abort(read_dep_txn_tid)) {
        // cascading abort
        return false;

      } else if (read_dep_txn->is_commit(read_dep_txn_tid)) {
        // check: version unchanged since out dependent txn has committed
        // check: there should not be a new writter in front of us
        if (read_dep_txn->get_commit_tid() != tuple->version || de)
            return false;

      } else {
        // check: since we read a dirty value from Txn T, T is still ongoing, we has two situation:
        // a. do not find a latest writer, the txn could have committed and gc the access entry, then check the record's version
        // b. find a latest writer, then make sure the dirtyRead has read its uncommitted value
        if (!de && (read_dep_txn->get_commit_tid() != tuple->version))
          return false;
        if (de) {
          Transaction *d_txn = (Transaction *) de->get_txn();
          if (read_dep_txn_tid != de->get_tid() || txn->read_set[pos].get_dep_write_seq() != de->get_write_seq())
            return false;
        }

      }
    }
    // end: early valiation for read

    // track dependency
    if (likely(de)) {
      Transaction *d_txn = (Transaction *) de->get_txn();
      uint64_t d_tid = de->get_tid();

      if (!d_txn->is_commit(d_tid)
          && d_tid != txn->get_tid()
          && d_txn != txn) {
        txn->add_dep_txn(de->get_tid(), de->get_txn(), true/*dirty_read_dep*/);
      }
    }
  }

  // insert op dependency
  if(is_write && txn->write_set[pos].is_insert()) {

    auto last_it = txn->write_set[pos].get_insert_node()->get_last_txn();
    uint64_t d_tid = last_it.first;
    Transaction* d_txn = (Transaction*)last_it.second;
    
    if(d_txn != nullptr) {
      varkey insk(txn->write_set[pos].get_key());
      varkey lowk(*txn->write_set[pos].get_insert_node()->low_key);
      varkey upk(*txn->write_set[pos].get_insert_node()->up_key);

      bool is_conflict = (insk >= lowk && insk <= upk);

      if(!d_txn->is_commit(d_tid)
        && is_conflict
        && d_tid != txn->get_tid()
        && d_txn != txn) {
          txn->add_dep_txn(d_tid, (void *)d_txn); 
      }
    }
  }

  if (!is_write && txn->read_set[pos].is_occ()) return true;
  if (!is_write && entry->is_overwrite()) return true;

  if (is_write) entry->set_write_seq(pos);
  tuple->append_entry(entry);
  entry->set_linked();
  entry->set_prepare(tuple);

  // tuple->increase_counter();

  return true;
}

//=================================RAW Operation================================

template <typename Transaction>
raw_op<Transaction>::raw_op(Transaction* t): txn(t)
{}

template <typename Transaction>
raw_op<Transaction>::~raw_op() {}


template <typename Transaction>
void raw_op<Transaction>::begin_piece()
{
  ALWAYS_ASSERT(false);
}


template <typename Transaction>
bool raw_op<Transaction>::commit_piece()
{
  ALWAYS_ASSERT(false);
  return true;
}

template <typename Transaction>
void raw_op<Transaction>::abort_piece(bool b )
{
  return;
}


 template <typename Transaction>
 template <typename ValueReader>
 bool raw_op<Transaction>::do_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader)
 {

   concurrent_btree::versioned_node_t search_info;
   typename concurrent_btree::value_type underlying_v{};

   bool found = btr.search(varkey(*key_str), underlying_v, &search_info);

   if(!found)
     return false;

   dbtuple *tuple = reinterpret_cast<dbtuple *>(underlying_v);

   return do_tuple_read(tuple, value_reader);

 }

 template <typename Transaction>
 template <typename ValueReader>
 bool raw_op<Transaction>::do_tuple_read(dbtuple* tuple, ValueReader& reader)
 {
   INVARIANT(!txn->is_snapshot());

   if(tuple == nullptr)
     return false;

   tuple->prefetch();
   return tuple->raw_stable_read(reader, txn->string_allocator());


 }

 template <typename Transaction>
 bool raw_op<Transaction>::do_node_read(
     const typename concurrent_btree::node_opaque_t *n, uint64_t v, bool occ)
 {
   return true;
 }


 template <typename Transaction>
 template <typename ValueWriter>
 bool raw_op<Transaction>::do_put(
     concurrent_btree &btr,
     const std::string* k,
     void* v,
     ValueWriter& writer,
     bool expect_new,
     bool visible)
 {

   bool insert = false;

   retry:

   dbtuple* tuple = nullptr;

   if(expect_new) {
     auto ret = try_insert_new_tuple(btr, k, v, writer);
     tuple = ret.first;

     if(tuple) {
       //The insert tuples should not be observed by concurrent txns
       tuple->set_pid(dbtuple::MIN_TID);
#ifndef USE_MCS_LOCK
       INVARIANT(tuple->is_locked());
      tuple->unlock();
#endif
       insert = true;
     }
   }

   if(!tuple) {
     // do regular search
     typename concurrent_btree::value_type bv = 0;
     if (!btr.search(varkey(*k), bv)) {
       expect_new = true;
       goto retry;
     }

     tuple = reinterpret_cast<dbtuple *>(bv);
   }

   //FIXME: only allocate the access entry in write set
   access_entry *entry = txn->insert_read_set(
       tuple, tuple->get_pid(), dbtuple::MIN_TID, txn->get_tid(), txn);

   txn->insert_write_set(tuple, k, v, writer, &btr, insert, entry);
   entry->set_prepare(tuple);

   if(insert) {
     entry->set_insert();
   }

   if(!v) {
     entry->set_delete();
   }

   entry->write(v, dbtuple::MIN_TID, txn->string_allocator());

   if(visible) {

#ifdef USE_MCS_LOCK
     tuple->mcs_lock(&entry->m_node);
#else
     tuple->lock();
#endif

     tuple->append_entry(entry);
     entry->set_linked();

#ifdef USE_MCS_LOCK
     tuple->mcs_unlock(&entry->m_node);
#else
     tuple->unlock();
#endif

   }

   return true;
 }


 template <typename Transaction>
 template <typename ValueWriter>
 std::pair< dbtuple *, bool >
 raw_op<Transaction>::try_insert_new_tuple(
     concurrent_btree &btr,
     const std::string* key,
     void* value,
     ValueWriter& writer,
     bool occ)
 {

   INVARIANT(value);

   const size_t sz =
       value ? writer(dbtuple::TUPLE_WRITER_COMPUTE_NEEDED,
                      value, nullptr, 0) : 0;

#ifdef USE_MCS_LOCK
   dbtuple * const tuple = dbtuple::alloc_first(sz, false);
#else
   //First alloc: version/subversion - MAXID
  dbtuple * const tuple = dbtuple::alloc_first(sz, true);
#endif
   //fprintf(stderr, "[atomic_mul_ops<Transaction>::try_insert_new_tuple] alloc tuple %lx\n", (uint64_t)tuple);

   if (value)
     writer(dbtuple::TUPLE_WRITER_DO_WRITE, value, tuple->get_value_start(), 0);

   typename concurrent_btree::insert_info_t insert_info;
   if (unlikely(!btr.insert_if_absent(
       varkey(*key), (typename concurrent_btree::value_type) tuple, &insert_info))) {
     VERBOSE(std::cerr << "insert_if_absent failed for key: " << util::hexify(key) << std::endl);

     tuple->clear_latest();
     tuple->unlock();

     //fprintf(stderr, "core[%d] insert_if_absent delete tuple %lx\n", coreid::core_id(), tuple );
     dbtuple::release_no_rcu(tuple);
     return std::pair< dbtuple *, bool >(nullptr, false);
   }

   return std::make_pair(tuple, false);
 }


//=================================Piece including one tuple access================================
template <typename Transaction>
atomic_one_op<Transaction>::atomic_one_op(Transaction* t): txn(t)
{}

template <typename Transaction>
atomic_one_op<Transaction>::~atomic_one_op() {}


template <typename Transaction>
void atomic_one_op<Transaction>::begin_piece()
{
  //1. Get the piece id
  pid = txn->get_pid();

  //2. Save the current position of rs/ws
  rs_start = txn->read_set.size();
  ws_start = txn->write_set.size();
}


template <typename Transaction>
bool atomic_one_op<Transaction>::commit_piece()
{

  INVARIANT(txn->write_set.size() <= (ws_start + 1));
  
  for(int i = rs_start; i < txn->read_set.size(); i++) {
    
    // XXX(Conrad): No wait deps for occ  action
    if (txn->read_set[i].is_occ())
      continue;

    const dbtuple* tuple = txn->read_set[i].get_tuple();
    if(!tuple->is_latest()){
      goto do_abort;
    }

    if(tuple->get_pid() != txn->read_set[i].get_tid()) {
      fprintf(stderr, "size %ld start %ld\n", txn->read_set[i].get_tid(), tuple->get_pid());
      ALWAYS_ASSERT(false);
    }

    INVARIANT(tuple->get_pid() == txn->read_set[i].get_tid());

  }

  //3. Check btree versions have not changed
  if (!txn->absent_set.empty()) {
    auto it = txn->absent_set.begin();
    for (; it != txn->absent_set.end(); ++it) {
      const uint64_t v = concurrent_btree::ExtractVersionNumber(it->first);
      if (unlikely(v != it->second.version)) {
        goto do_abort;
      }
    }
  }

  //4. insert operations depend on range query and empty read
  // if (!txn->insert_set.empty()) {

  //   INVARIANT(txn->insert_set.size() == 1);
    
  //   auto it = txn->insert_set.begin();

  //   for (; it != txn->insert_set.end(); ++it) {
      
  //     // XXX(Conrad): Line 22 in Algorithm 1
  //     auto last_it = it->node->get_last_txn();

  //     uint64_t d_tid = last_it.first;
  //     Transaction* d_txn = (Transaction*)last_it.second;
      
  //     if(d_txn == nullptr)
  //       continue;
      
  //     if(!d_txn->is_commit(d_tid) 
  //       && d_tid != txn->get_tid()
  //       && d_txn != txn) {
        
  //       //FIXME: profiling purpose
  //       INVARIANT(txn->get_type() != 1 || d_txn->get_type() == 3);

  //       txn->add_dep_txn(d_tid, (void *)d_txn); 
  //     }
  //   }
  // }

  //5. Stamp all the tree node in the absent set
  if (!txn->absent_set.empty()) {
    auto it = txn->absent_set.begin();
    for (; it != txn->absent_set.end(); ++it) {
      concurrent_btree::node_opaque_t *node = const_cast<concurrent_btree::node_opaque_t *>(it->first);
      node->set_last_txn(txn->get_tid(), txn, it->second.low_key, it->second.up_key);
    }
  }  


  //6. Linked all entries of all tuples in both read set and write set
  for(int i = rs_start; i < txn->read_set.size(); i++) {

    dbtuple* tuple = const_cast<dbtuple*>(txn->read_set[i].get_tuple());
    access_entry* e = txn->read_set[i].get_access_entry();
    
    // access_entry* de = tuple->get_latest_active_entry();
    access_entry* de = tuple->get_dependent_entry(e->is_write());

    // XXX(Conrad): OCC actions should not generate dependencies
    if(de && !txn->read_set[i].is_occ()) {
      Transaction* d_txn = (Transaction*)de->get_txn();
      uint64_t d_tid = de->get_tid();

      // txn->read_set[i].set_dep_txn(de->get_txn());

      if(!d_txn->is_commit(d_tid) 
        && d_tid != txn->get_tid()
        && d_txn != txn) {
        txn->add_dep_txn(de->get_tid(), de->get_txn()); 
      }
    }

    tuple->append_entry(e);
    e->set_linked();
    tuple->set_pid(e->get_pid());

    if(tuple->is_first_active_entry(e) 
      && !e->is_insert()) {
      e->set_prepare(tuple);
    } else if((!tuple->is_first_active_entry(e)) && !de) {
      e->set_prepare(tuple);
    }

    // XXX(Conrad): If OCC action is taken the raw_op::do_get is executed, which
    // does not perform mcs_lock
    if (!txn->read_set[i].is_occ()) {
#ifdef USE_MCS_LOCK
      tuple->mcs_unlock(&e->m_node);
#else
      tuple->unlock();
#endif
    }

  }

  clean_up();

  //fprintf(stderr, "core[%d] Finish Commit Piece\n", coreid::core_id());

  return true;

do_abort:
  
  abort_piece(true);
  
  return false;
}



template <typename Transaction>
void atomic_one_op<Transaction>::abort_piece(bool unlock_all = false)
{
  for (int i = ws_start; i < txn->write_set.size(); i++) {
    if (txn->write_set[i].is_insert()) {
      dbtuple * const tuple = txn->write_set[i].get_tuple();

      INVARIANT(tuple->is_locked() && tuple->access_head == NULL);
      //fprintf(stderr, "core[%d] clean up tuple %lx\n", coreid::core_id(), tuple);
      txn->cleanup_inserted_tuple_marker(tuple, txn->write_set[i].get_key(), txn->write_set[i].get_btree());
      if(!unlock_all) {
#ifdef USE_MCS_LOCK
        access_entry* e = txn->write_set[i].get_access_entry();
        tuple->mcs_unlock(&e->m_node);
#else        
        tuple->unlock();
#endif    
      }
    }
  }
  
  if(unlock_all)
    txn->release_rs_locks(rs_start);

  txn->read_set.shrink(rs_start);
  txn->write_set.shrink(ws_start);
  clean_up();
}

template <typename Transaction>
void atomic_one_op<Transaction>::clean_up()
{
  // txn->insert_set.clear();
  txn->absent_set.clear();
}


template <typename Transaction>
template <typename ValueReader> 
bool atomic_one_op<Transaction>::do_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader)
{

  concurrent_btree::versioned_node_t search_info;
  typename concurrent_btree::value_type underlying_v{};

  bool found = btr.search(varkey(*key_str), underlying_v, &search_info);

  if(!found) {
    //TODO: put the search info into absent set
    do_node_read(search_info.first, search_info.second, key_str, key_str, true);
    return false;
  }

  
  dbtuple* tuple = reinterpret_cast<dbtuple *>(underlying_v);
  
  return do_tuple_read(tuple, value_reader);

}

template <typename Transaction>
template <typename ValueReader> 
bool atomic_one_op<Transaction>::profile_do_get(concurrent_btree &btr, 
  const std::string* key_str, ValueReader& value_reader, ic3_profile* prof)
{

  uint64_t beg = rdtsc();

  concurrent_btree::versioned_node_t search_info;
  typename concurrent_btree::value_type underlying_v{};

  bool found = btr.search(varkey(*key_str), underlying_v, &search_info);

  if(!found) {
    //TODO: put the search info into absent set
    do_node_read(search_info.first, search_info.second, key_str, key_str, true);
    return false;
  }

  
  dbtuple* tuple = reinterpret_cast<dbtuple *>(underlying_v);
  
  bool res = profile_do_tuple_read(tuple, value_reader, prof);

  prof->data[ic3_profile::level::LEVEL2] += rdtsc() - beg;

  return res;

}

template <typename Transaction>
template <typename ValueReader> 
bool atomic_one_op<Transaction>::profile_do_tuple_read(dbtuple* tuple, 
                                  ValueReader& value_reader, ic3_profile* prof)
{
  INVARIANT(!txn->is_snapshot());

  uint64_t beg = rdtsc();

  access_entry* e = read_set_find(tuple);
  if(e!=NULL)
    return e->read_record(value_reader, txn->string_allocator());
  

  tuple->prefetch();

  e = txn->insert_read_set(tuple, tuple->get_pid(), pid, txn->get_tid(), txn);

#ifdef USE_MCS_LOCK
  tuple->mcs_lock(&e->m_node);
  txn->read_set.back().reset_tid(tuple->get_pid());
#else
  tuple->lock(true);
#endif

  uint64_t beg1 = rdtsc();

  bool res = tuple->profile_ic3_read(value_reader, txn->string_allocator(), e, prof);

  prof->data[ic3_profile::level::LEVEL3] += rdtsc() - beg;
  prof->data[ic3_profile::level::LEVEL4] += rdtsc() - beg1;

  return res;
}

template <typename Transaction>
template <typename ValueReader> 
bool atomic_one_op<Transaction>::do_tuple_read(
    dbtuple* tuple, ValueReader& value_reader)
{
  INVARIANT(!txn->is_snapshot());

  access_entry* e = read_set_find(tuple);
  if(e!=NULL)
    return e->read_record(value_reader, txn->string_allocator());
  

  tuple->prefetch();

  e = txn->insert_read_set(tuple, tuple->get_pid(), pid, txn->get_tid(), txn);

  // XXX(Conrad): Do we want locking while mixing, even for IC3? This is
  // essentially the lock_guard issue in RocksDB TPCC
#ifdef USE_MCS_LOCK
  tuple->mcs_lock(&e->m_node);
  txn->read_set.back().reset_tid(tuple->get_pid());
#else
  tuple->lock(true);
#endif

  return tuple->ic3_read(value_reader, txn->string_allocator(), e);
}

template <typename Transaction>
access_entry* atomic_one_op<Transaction>::read_set_find(dbtuple* tuple)
{
  for(int i = rs_start; i < txn->read_set.size(); i++) {
    if(txn->read_set[i].get_tuple() == tuple) {
      return txn->read_set[i].get_access_entry();
    }
  }
  return nullptr;
}

template <typename Transaction>
bool atomic_one_op<Transaction>::do_node_read(const typename concurrent_btree::node_opaque_t *n, 
                                          uint64_t v, const std::string* lkey, const std::string* ukey, bool occ)
{
  auto it = txn->absent_set.find(n);

  if(it == txn->absent_set.end()) {
    //fprintf(stderr, "Try to put %lx [%d] \n", (uint64_t)n, txn->absent_set.size());
    txn->absent_set[n].version = v;
    txn->absent_set[n].low_key = lkey;
    txn->absent_set[n].up_key = ukey;
  } else if (it->second.version != v) {
    const piece_base::abort_reason r = piece_base::ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED;
    abort_piece();
    throw piece_abort_exception(r);
  }

  return true;
}


template <typename Transaction>
template <typename ValueWriter> 
bool atomic_one_op<Transaction>::do_put(
      concurrent_btree &btr, 
      const std::string* k,
      void* v, 
      ValueWriter& writer,
      bool expect_new)

{
  
  INVARIANT(!expect_new || v);

  dbtuple *px = nullptr;

retry:
  
  if(expect_new) {
    
    auto ret = try_insert_new_tuple(btr, k, v, writer);
    px = ret.first;

    if(px) {
      INVARIANT(px->is_locked());
      access_entry* e = txn->insert_read_set(px, px->get_pid(), pid, txn->get_tid(), txn);

#ifdef USE_MCS_LOCK
      px->mcs_lock(&e->m_node);
      txn->read_set.back().reset_tid(px->get_pid());
#endif
      

      txn->insert_write_set(px, k, v, writer, &btr, true, e);
      e->write(v, pid, txn->string_allocator());
      e->set_insert();
      e->set_prepare(px); 
      
      if(unlikely(ret.second)) {
        abort_piece();
        const piece_base::abort_reason r = piece_base::ABORT_REASON_INSERT_FAILED;
        throw piece_abort_exception(r);
      }
      
    }
  } 

  if(!px) {
    // do regular search
    typename concurrent_btree::value_type bv = 0;
    if (!btr.search(varkey(*k), bv)) {
      expect_new = true;
      goto retry;
    }

    px = reinterpret_cast<dbtuple *>(bv);

    bool ret = px->is_initial();

    access_entry* e = read_set_find(px);

    if(!(ret || (e && e->is_insert()))) {
        ret = px->wait_initial();
        if(!ret)
          goto retry;
    }

    
    if(!e) {
      e = txn->insert_read_set(px, px->get_pid(), pid, txn->get_tid(), txn);

#ifdef USE_MCS_LOCK
      px->mcs_lock(&e->m_node);
      txn->read_set.back().reset_tid(px->get_pid());
#else
      px->lock(true);
#endif

    }

    if(!e->is_write())
      txn->insert_write_set(px, k, v, writer, &btr, false, e);
    else
      e->set_overwrite();

    if(!v) {
      e->set_delete();
    }

    e->write(v, pid, txn->string_allocator());
  }

  return true;
}

template <typename Transaction>
template <typename ValueWriter> 
std::pair< dbtuple *, bool >
atomic_one_op<Transaction>::try_insert_new_tuple(
      concurrent_btree &btr, 
      const std::string* key,
      void* value, 
      ValueWriter& writer,
      bool occ)
{
  
  INVARIANT(value);

  const size_t sz =
    value ? writer(dbtuple::TUPLE_WRITER_COMPUTE_NEEDED,
      value, nullptr, 0) : 0;


#ifdef USE_MCS_LOCK
  dbtuple * const tuple = dbtuple::alloc_first(sz, false);
#else    
  //First alloc: version/subversion - MAXID
  dbtuple * const tuple = dbtuple::alloc_first(sz, true);
#endif


  //fprintf(stderr, "[atomic_mul_ops<Transaction>::try_insert_new_tuple] alloc tuple %lx\n", (uint64_t)tuple);

  if (value) 
    writer(dbtuple::TUPLE_WRITER_DO_WRITE, value, tuple->get_value_start(), 0);

  typename concurrent_btree::insert_info_t insert_info;
  if (unlikely(!btr.insert_if_absent(
          varkey(*key), (typename concurrent_btree::value_type) tuple, &insert_info))) {
    VERBOSE(std::cerr << "insert_if_absent failed for key: " << util::hexify(key) << std::endl);

    tuple->clear_latest();
    tuple->unlock();

    //fprintf(stderr, "core[%d] insert_if_absent delete tuple %lx\n", coreid::core_id(), tuple );
    dbtuple::release_no_rcu(tuple);
    return std::pair< dbtuple *, bool >(nullptr, false);
  }

  //fprintf(stderr, "Insert Succ\n");
  if (!txn->absent_set.empty()) {
    //fprintf(stderr, "Try to find %lx\n", (uint64_t)insert_info.node);
    auto it = txn->absent_set.find(insert_info.node);
    if (it != txn->absent_set.end()) {
      if (unlikely(it->second.version != insert_info.old_version)) {
        return std::make_pair(tuple, true);
      }
      // otherwise, bump the version
      it->second.version = insert_info.new_version;
    }
  }


  // txn->put_insert_set(
  //   const_cast<concurrent_btree::node_opaque_t *>(insert_info.node), key);

  return std::make_pair(tuple, false);
}


//=================================Piece including multiple tuples access================================

template <typename Transaction>
atomic_mul_ops<Transaction>::atomic_mul_ops(Transaction* t): txn(t)
{}

template <typename Transaction>
atomic_mul_ops<Transaction>::~atomic_mul_ops(){}


template <typename Transaction>
void atomic_mul_ops<Transaction>::begin_piece()
{
  //fprintf(stderr, "core[%d] Start Piece\n", coreid::core_id());

  //1. Get the piece id
  pid = txn->get_pid();

  //fprintf(stderr, "piece id %lx\n", pid);

  //2. Save the current position of rs/ws
  rs_start = txn->read_set.size();
  ws_start = txn->write_set.size();
}

template <typename Transaction>
bool atomic_mul_ops<Transaction>::commit_piece()
{
  //fprintf(stderr, "core[%d] Begin Commit Piece\n", coreid::core_id());

  //We assume all tuples will be in the readset
  txn->acquire_rs_locks(rs_start);

  //2. Check the read set
  for(int i = rs_start; i < txn->read_set.size(); i++) {
    
    // XXX(Conrad): No wait deps for occ  action
    if (txn->read_set[i].is_occ())
      continue;

    const dbtuple* tuple = txn->read_set[i].get_tuple();

    if(tuple->get_pid() != txn->read_set[i].get_tid() 
      || !tuple->is_latest()) {
      goto do_abort;
    }

    access_entry* entry = txn->read_set[i].get_access_entry();
    if(entry->is_write() && !tuple->is_latest())
        goto do_abort;
  }

  //3. Check btree versions have not changed
  if (!txn->absent_set.empty()) {

    auto it = txn->absent_set.begin();
    for (; it != txn->absent_set.end(); ++it) {
      const uint64_t v = concurrent_btree::ExtractVersionNumber(it->first);
      if (unlikely(v != it->second.version)) {
        goto do_abort;
      }
    }

  }

  //fprintf(stderr, "[commit_piece]: core[%d] txn [%lx] commit piece [%lx]\n", 
    //coreid::core_id(), txn->get_tid(), pid);

  //4. insert operations depend on range query and empty read
  // if (!txn->insert_set.empty()) {

  //   auto it = txn->insert_set.begin();

  //   for (; it != txn->insert_set.end(); ++it) {
      
  //     auto last_it = it->node->get_last_txn();

  //     uint64_t d_tid = last_it.first;
  //     Transaction* d_txn = (Transaction*)last_it.second;
      
  //     if(d_txn == nullptr)
  //       continue;
      
  //     varkey insk(*it->key);
  //     varkey lowk(*it->node->low_key);
  //     varkey upk(*it->node->up_key);

  //     bool is_conflict = (insk >= lowk && insk <= upk);

  //     if(!d_txn->is_commit(d_tid)
  //       && is_conflict
  //       && d_tid != txn->get_tid()
  //       && d_txn != txn) {
  //         txn->add_dep_txn(d_tid, (void *)d_txn); 
  //     }
  //   }
  // }

  //5. Stamp all the tree node in the absent set
  if (!txn->absent_set.empty()) {
    auto it = txn->absent_set.begin();
    for (; it != txn->absent_set.end(); ++it) {
      concurrent_btree::node_opaque_t *node = const_cast<concurrent_btree::node_opaque_t *>(it->first);
      node->set_last_txn(txn->get_tid(), txn, it->second.low_key, it->second.up_key);
    }
  }  


  //6. Linked all entries of all tuples in both read set and write set
  for(int i = rs_start; i < txn->read_set.size(); i++) {

    dbtuple* tuple = const_cast<dbtuple*>(txn->read_set[i].get_tuple());
    access_entry* e = txn->read_set[i].get_access_entry();
    
    // access_entry* de = tuple->get_latest_active_entry();
    access_entry* de = tuple->get_dependent_entry(e->is_write());

    // XXX(Conrad): OCC actions should not generate dependencies
    if(de && !txn->read_set[i].is_occ()) {
      Transaction* d_txn = (Transaction*)de->get_txn();
      uint64_t d_tid = de->get_tid();

      // txn->read_set[i].set_dep_txn(de->get_txn());

      // if(txn->get_type() == 3 && e->is_write()) {
      //     fprintf(stderr, "[commit piece2] core[%d] t[%d]:<%lx, %lx> -- %d dep on t[%d]:<%lx/%lx, %lx> state %d last_de %lx when append_entry %lx\n", 
      //       coreid::core_id(), txn->get_type(), txn->get_tid(), txn, txn->get_cstep(),
      //       d_txn->get_type(), d_tid, d_txn->get_tid(), d_txn, d_txn->state, de, e);
      // }

      if(!d_txn->is_commit(d_tid) 
        && d_tid != txn->get_tid()
        && d_txn != txn) {
        txn->add_dep_txn(de->get_tid(), de->get_txn()); 
      }
    }

    
    tuple->append_entry(e);

    e->set_linked();
    
    if(e->is_write())
      tuple->set_pid(e->get_pid());

    if(tuple->is_first_active_entry(e) 
      && !e->is_insert()) {
      e->set_prepare(tuple);
    } else if((!tuple->is_first_active_entry(e)) && !de) {
      e->set_prepare(tuple);
    }
    // XXX(Conrad): If OCC action is taken the raw_op::do_get is executed, which
    // does not perform mcs_lock
    if (!txn->read_set[i].is_occ()) {
#ifdef USE_MCS_LOCK
      tuple->mcs_unlock(&e->m_node);
#else
      tuple->unlock();
#endif
    }

  }

  clean_up();

  //fprintf(stderr, "core[%d] Finish Commit Piece\n", coreid::core_id());

  return true;

do_abort:
  
  abort_piece(true);
  
  return false;
}



template <typename Transaction>
void atomic_mul_ops<Transaction>::abort_piece(bool unlock_all = false)
{
  //fprintf(stderr, "core[%d] Abort Piece\n", coreid::core_id());

  //Reset the txn's read/write set cursor
  for (int i = ws_start; i < txn->write_set.size(); i++) {
    if (txn->write_set[i].is_insert()) {
      dbtuple * const tuple = txn->write_set[i].get_tuple();

      INVARIANT(tuple->is_locked() && tuple->access_head == NULL);
      //fprintf(stderr, "core[%d] clean up tuple %lx\n", coreid::core_id(), tuple);
      txn->cleanup_inserted_tuple_marker(tuple, txn->write_set[i].get_key(), txn->write_set[i].get_btree());
      if(!unlock_all) {
#ifdef USE_MCS_LOCK
        access_entry* e = txn->write_set[i].get_access_entry();
        tuple->mcs_unlock(&e->m_node);
#else        
        tuple->unlock();
#endif
      }
    }
  }
  
  if(unlock_all)
    txn->release_rs_locks(rs_start);

  txn->read_set.shrink(rs_start);
  txn->write_set.shrink(ws_start);
  clean_up();
}

template <typename Transaction>
void atomic_mul_ops<Transaction>::clean_up()
{
  // txn->insert_set.clear();
  txn->absent_set.clear();
}


template <typename Transaction>
bool atomic_mul_ops<Transaction>::own_insert(dbtuple* tuple)
{
  access_entry* e = read_set_find(tuple);
  return (e && e->is_insert());  
}


template <typename Transaction>
template <typename ValueReader> 
bool atomic_mul_ops<Transaction>::do_get(concurrent_btree &btr, 
      const std::string* key_str, ValueReader& value_reader)
{

  concurrent_btree::versioned_node_t search_info;
  typename concurrent_btree::value_type underlying_v{};

retry:
  bool found = btr.search(varkey(*key_str), underlying_v, &search_info);

  if(!found) {
    //TODO: put the search info into absent set
    do_node_read(search_info.first, search_info.second, key_str, key_str, true);
    return false;
  }

  
  dbtuple* tuple = reinterpret_cast<dbtuple *>(underlying_v);
  
  bool ret = tuple->is_initial();

  if(!(ret || own_insert(tuple))) {
      ret = tuple->wait_initial();
      if(!ret)
        goto retry;
  }
  

  return do_tuple_read(tuple, value_reader);
}




template <typename Transaction>
template <typename ValueWriter> 
std::pair< dbtuple *, bool >
atomic_mul_ops<Transaction>::try_insert_new_tuple(
      concurrent_btree &btr, 
      const std::string* key,
      void* value, 
      ValueWriter& writer,
      bool occ)
{
  
  INVARIANT(value);

  const size_t sz =
    value ? writer(dbtuple::TUPLE_WRITER_COMPUTE_NEEDED,
      value, nullptr, 0) : 0;

  //First alloc: version/subversion - MAXID

#ifdef USE_MCS_LOCK
  dbtuple * const tuple = dbtuple::alloc_first(sz, false);
#else    
  //First alloc: version/subversion - MAXID
  dbtuple * const tuple = dbtuple::alloc_first(sz, true);
#endif


  //fprintf(stderr, "[atomic_mul_ops<Transaction>::try_insert_new_tuple] alloc tuple %lx\n", (uint64_t)tuple);

  if (value) 
    writer(dbtuple::TUPLE_WRITER_DO_WRITE, value, tuple->get_value_start(), 0);

  typename concurrent_btree::insert_info_t insert_info;
  if (unlikely(!btr.insert_if_absent(
          varkey(*key), (typename concurrent_btree::value_type) tuple, &insert_info))) {
    VERBOSE(std::cerr << "insert_if_absent failed for key: " << util::hexify(key) << std::endl);

    tuple->clear_latest();
    tuple->unlock();

    //fprintf(stderr, "core[%d] insert_if_absent delete tuple %lx\n", coreid::core_id(), tuple );
    dbtuple::release_no_rcu(tuple);
    return std::pair< dbtuple *, bool >(nullptr, false);
  }

  //fprintf(stderr, "Insert Succ\n");
  if (!txn->absent_set.empty()) {
    //fprintf(stderr, "Try to find %lx\n", (uint64_t)insert_info.node);
    auto it = txn->absent_set.find(insert_info.node);
    if (it != txn->absent_set.end()) {
      if (unlikely(it->second.version != insert_info.old_version)) {
        return std::make_pair(tuple, true);
      }
      // otherwise, bump the version
      it->second.version = insert_info.new_version;
    }
  }


  // txn->put_insert_set(
    // const_cast<concurrent_btree::node_opaque_t *>(insert_info.node), key);

  return std::make_pair(tuple, false);
}

template <typename Transaction>
access_entry* atomic_mul_ops<Transaction>::read_set_find(dbtuple* tuple)
{
  for(int i = rs_start; i < txn->read_set.size(); i++) {
    if(txn->read_set[i].get_tuple() == tuple) {
      return txn->read_set[i].get_access_entry();
    }
  }
  return nullptr;
}


template <typename Transaction>
template <typename ValueWriter> 
bool atomic_mul_ops<Transaction>::do_put(
      concurrent_btree &btr, 
      const std::string* k,
      void* v, 
      ValueWriter& writer,
      bool expect_new)
{
  
  INVARIANT(!expect_new || v);

  dbtuple *px = nullptr;

retry:
  
  if(expect_new) {
    
    auto ret = try_insert_new_tuple(btr, k, v, writer);
    px = ret.first;

    if(px) {
      access_entry* e = txn->insert_read_set(px, px->get_pid(), pid, txn->get_tid(), txn);

#ifdef USE_MCS_LOCK
      px->mcs_lock(&e->m_node);
      txn->read_set.back().reset_tid(px->get_pid());
#else
      INVARIANT(px->is_locked());      
#endif

      txn->insert_write_set(px, k, v, writer, &btr, true, e);
      e->write(v, pid, txn->string_allocator());
      e->set_insert();
      e->set_prepare(px); 
      
      if(unlikely(ret.second)) {
        abort_piece();
        const piece_base::abort_reason r = piece_base::ABORT_REASON_INSERT_FAILED;
        throw piece_abort_exception(r);
      }
      
    }
  } 

  if(!px) {
    // do regular search
    typename concurrent_btree::value_type bv = 0;
    if (!btr.search(varkey(*k), bv)) {
      expect_new = true;
      goto retry;
    }

    px = reinterpret_cast<dbtuple *>(bv);

    bool ret = px->is_initial();

    if(!(ret || own_insert(px))) {
        ret = px->wait_initial();
        if(!ret)
          goto retry;
    }

    access_entry* e = read_set_find(px);
    if(!e) {
      e = txn->insert_read_set(px, px->get_pid(), pid, txn->get_tid(), txn);
    }

    if(!e->is_write())
      txn->insert_write_set(px, k, v, writer, &btr, false, e);
    else
      e->set_overwrite();

    if(!v) {
      e->set_delete();
    }

    e->write(v, pid, txn->string_allocator());
  }

  return true;
}



template <typename Transaction>
bool atomic_mul_ops<Transaction>::do_node_read(const typename concurrent_btree::node_opaque_t *n, 
                                                uint64_t v, const std::string* lkey, const std::string* ukey, bool occ)
{
  auto it = txn->absent_set.find(n);

  if(it == txn->absent_set.end()) {
    //fprintf(stderr, "Try to put %lx [%d] \n", (uint64_t)n, txn->absent_set.size());
    txn->absent_set[n].version = v;
    txn->absent_set[n].low_key = lkey;
    txn->absent_set[n].up_key = ukey;
  } else if (it->second.version != v) {
    const piece_base::abort_reason r = piece_base::ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED;
    abort_piece();
    throw piece_abort_exception(r);
  }

  return true;
}


//Buffer the value read in the local access entry
template <typename Transaction>
template <typename ValueReader> 
bool atomic_mul_ops<Transaction>::do_tuple_read(
    dbtuple* tuple, ValueReader& value_reader)
{

  INVARIANT(!txn->is_snapshot());

  //Assume all the tuples in the read set
  access_entry* e = read_set_find(tuple);
  if(e!=NULL)
    return e->read_record(value_reader, txn->string_allocator());
  

  tuple->prefetch();

  e = txn->insert_read_set(tuple, tuple->get_pid(), pid, txn->get_tid(), txn);

  dbtuple::tid_t pid;

  bool res = tuple->ic3_stable_read(value_reader, pid, txn->string_allocator(), e);

  txn->read_set.back().reset_tid(pid);
  return res;
}





template <typename Transaction>
op_wrapper<Transaction>::op_wrapper(Transaction* t)
:r_op(t), m_op(t), o_op(t), mix_op(t), op_s(MIX) {}

template <typename Transaction>
op_wrapper<Transaction>::~op_wrapper(){}

template <typename Transaction>
template <typename ValueReader> 
bool op_wrapper<Transaction>::do_tuple_read(
    dbtuple* tuple, ValueReader& reader, bool occ)
{
  return mix_op.do_tuple_read(tuple, reader);
//  switch(op_s) {
//    case RAW:
//      return r_op.do_tuple_read(tuple, reader);
//    case ONE_ATOMIC:
//      return o_op.do_tuple_read(tuple, reader);
//    case MUL_ATOMIC:
//      return m_op.do_tuple_read(tuple, reader);
//    case MIX:
//      return mix_op.do_tuple_read(tuple, reader);
//    default:
//      ALWAYS_ASSERT(false);
//  }
//  return false;
}


template <typename Transaction>
bool op_wrapper<Transaction>::do_node_read(const typename concurrent_btree::node_opaque_t *n, 
                                            uint64_t v, const std::string* lkey, const std::string* ukey, bool occ)
{
  return mix_op.do_node_read(n, v, lkey, ukey, occ);
//  switch(op_s) {
//    case RAW:
//      return r_op.do_node_read(n, v);
//    case ONE_ATOMIC:
//      return o_op.do_node_read(n, v, lkey, ukey);
//    case MUL_ATOMIC:
//      return m_op.do_node_read(n, v, lkey, ukey);
//    case MIX:
//      return mix_op.do_node_read(n, v, lkey, ukey);
//    default:
//      ALWAYS_ASSERT(false);
//  }
//  return false;
}

template <typename Transaction>
template <typename ValueReader> 
bool op_wrapper<Transaction>::do_get(concurrent_btree &btr, const std::string* key, ValueReader& reader)
{
  ALWAYS_ASSERT(false);
  // return mix_op.do_get(btr, key, reader, decision, lock_mode, expose);
//  switch(op_s) {
//    case RAW:
//      return r_op.do_get(btr, key, reader);
//    case ONE_ATOMIC:
//      return o_op.do_get(btr, key, reader);
//    case MUL_ATOMIC:
//      return m_op.do_get(btr, key, reader);
//    case MIX:
//      return mix_op.do_get(btr, key, reader, occ);
//    default:
//      ALWAYS_ASSERT(false);
//  }
 return false;
}

template <typename Transaction>
template <typename ValueReader> 
bool op_wrapper<Transaction>::profile_do_get(concurrent_btree &btr, 
                const std::string* key, ValueReader& reader, ic3_profile* prof)
{
  ALWAYS_ASSERT(op_s == ONE_ATOMIC);
  return o_op.profile_do_get(btr, key, reader, prof);
  
}


template <typename Transaction>
template <typename ValueWriter> 
bool op_wrapper<Transaction>::do_put(concurrent_btree &btr, const std::string* key, 
                                  void* val, ValueWriter& writer, bool expect_new)
{
  ALWAYS_ASSERT(false);
  // return mix_op.do_put(btr, key, val, writer, expect_new, occ, lock_mode, expose);
//    INVARIANT(!visible || op_s == RAW);
//
//    switch(op_s) {
//      case RAW:
//        return r_op.do_put(btr, key, val, writer, expect_new, visible);
//      case ONE_ATOMIC:
//        return o_op.do_put(btr, key, val, writer, expect_new);
//      case MUL_ATOMIC:
//        return m_op.do_put(btr, key, val, writer, expect_new);
//      case MIX:
//        return mix_op.do_put(btr, key, val, writer, expect_new, occ);
//      default:
//        ALWAYS_ASSERT(false);
//    }
   return false;
}

 template <typename Transaction>
 template <typename ValueReader, typename ValueWriter>
 bool op_wrapper<Transaction>::do_update(concurrent_btree &btr, const std::string *key_str,
                                         ValueReader &value_reader, update_callback *callback,
                                         ValueWriter &value_writer)
 {
   ALWAYS_ASSERT(false);
  //  return mix_op.do_update(btr, key_str, value_reader, callback, value_writer, occ, lock_mode, expose);
 }


template <typename Transaction>
void op_wrapper<Transaction>::begin_atomic_piece(bool one_tuple)
{
//  INVARIANT(op_s == RAW);
//
//  if(one_tuple) {
//    op_s = ONE_ATOMIC;
//    o_op.begin_piece();
//  }
//  else {
//    op_s = MUL_ATOMIC;
//    m_op.begin_piece();
//  }


  // XXX(Jiachen): Hack - everything simply use MIX
  // TODO - this hack need to changed by modifying benchmark code
  op_s = MIX;
} 

template <typename Transaction>
bool op_wrapper<Transaction>::commit_atomic_piece()
{
//  INVARIANT(op_s != RAW);
//
//  if(op_s == ONE_ATOMIC) {
//    op_s = RAW;
//    return o_op.commit_piece();
//  }
//  else if(op_s == MUL_ATOMIC) {
//    op_s = RAW;
//    return m_op.commit_piece();
//  }
//
//  INVARIANT(false);
//  return false;


  // XXX(Jiachen): Hack - everything simply use MIX
  // TODO - this hack need to changed by modifying benchmark code
  op_s = MIX;

  return true;
}

template <typename Transaction>
void op_wrapper<Transaction>::abort_piece()
{
  mix_op.abort_piece(true);
//  switch(op_s) {
//    case RAW:
//      return r_op.abort_piece(true);
//    case ONE_ATOMIC:
//      return o_op.abort_piece(true);
//    case MUL_ATOMIC:
//      return m_op.abort_piece(true);
//    case MIX:
//      return mix_op.abort_piece(true);
//    default:
//      ALWAYS_ASSERT(false);
//  }
}

template <typename Transaction>
bool op_wrapper<Transaction>::abort_atomic_piece()
{
//  INVARIANT(op_s != RAW);
//  op_s = RAW;
//  return true;

  // XXX(Jiachen): Hack - everything simply use MIX
  // TODO - this hack need to changed by modifying benchmark code
  op_s = MIX;
  return true;
}


#endif 


