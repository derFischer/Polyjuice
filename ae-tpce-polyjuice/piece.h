#ifndef _PIECE_H_
#define _PIECE_H_

#include <exception>
#include "policy.h"
#include "PolicyGradient.h"
#include "tuple.h"
#include "txn_entry.h"
#include "update_callback.h"


class piece_base {

public:

#define PABORT_REASONS(x) \
    x(ABORT_REASON_NONE) \
    x(ABORT_REASON_INSERT_FAILED) \
    x(ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED) \
    x(ABORT_REASON_EARLY_VALIDATION)

  enum abort_reason {
#define ENUM_X(x) x,
    PABORT_REASONS(ENUM_X)
#undef ENUM_X
  };

  static const char *
  AbortReasonStr(abort_reason reason)
  {
    switch (reason) {
#define CASE_X(x) case x: return #x;
    PABORT_REASONS(CASE_X)
#undef CASE_X
    default:
      break;
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

  piece_base(){}

  piece_base(const piece_base &) = delete;
  piece_base(piece_base &&) = delete;
  piece_base &operator=(const piece_base &) = delete;

};


class piece_abort_exception : public std::exception {
public:
  piece_abort_exception(piece_base::abort_reason r)
    : r(r) {}
  inline piece_base::abort_reason
  get_reason() const
  {
    return r;
  }
   const char *
  what() const throw()
  {
    return piece_base::AbortReasonStr(r);
  }
private:
  piece_base::abort_reason r;
};


//Non-conflict operations
template <typename Transaction>
class raw_op
{

public:
   raw_op(Transaction* t);
   ~raw_op();

  template <typename ValueReader> 
   bool do_tuple_read(dbtuple* tuple, ValueReader& value_reader);
   bool do_node_read(const typename concurrent_btree::node_opaque_t *n, uint64_t v, bool occ = true);

  template <typename ValueReader> 
  bool do_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader);
  template <typename ValueWriter> 
  bool do_put(concurrent_btree &btr, const std::string* key_str, 
              void* val_str, ValueWriter& value_writer, bool expect_new, bool visible);


  void begin_piece();
  void abort_piece(bool unlock);
  bool commit_piece();

protected:
  template <typename ValueWriter> 
  std::pair< dbtuple *, bool> 
  try_insert_new_tuple(concurrent_btree &btr, const std::string* key, void* value, ValueWriter& writer, bool occ = true);

private:
  Transaction* txn;

};


template <typename Transaction>
class atomic_one_op
{

public:
  atomic_one_op(Transaction* t);
  ~atomic_one_op();

  template <typename ValueReader> 
  bool do_tuple_read(dbtuple* tuple, ValueReader& value_reader);
  bool do_node_read(const typename concurrent_btree::node_opaque_t *n, 
                    uint64_t v, const std::string* lkey, const std::string* ukey, bool occ = true);

  template <typename ValueReader> 
  bool do_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader);
  template <typename ValueWriter> 
  bool do_put(concurrent_btree &btr, const std::string* key_str, 
          void* val_str, ValueWriter& value_writer, bool expect_new); 

  template <typename ValueReader> 
  bool profile_do_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader, ic3_profile* prof); 

  template <typename ValueReader> 
  bool profile_do_tuple_read(dbtuple* tuple, ValueReader& value_reader, ic3_profile* prof); 

  void begin_piece();
  void abort_piece(bool unlock);
  bool commit_piece();

protected:
  template <typename ValueWriter> 
  std::pair< dbtuple *, bool> 
  try_insert_new_tuple(concurrent_btree &btr, const std::string* key, void* value, ValueWriter& writer, bool occ = true);
  access_entry* read_set_find(dbtuple* tuple);

  void clean_up();
private:
  uint64_t pid;
  uint32_t rs_start;
  uint32_t ws_start;
  Transaction* txn;
};

template <typename Transaction>
class atomic_mul_ops
{

public:
  atomic_mul_ops(Transaction* t);
  ~atomic_mul_ops();

  template <typename ValueReader> 
  bool do_tuple_read(dbtuple* tuple, ValueReader& value_reader);
  bool do_node_read(const typename concurrent_btree::node_opaque_t *n, 
                      uint64_t v, const std::string* lkey, const std::string* ukey, bool occ = true);

  template <typename ValueReader> 
  bool do_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader);
  template <typename ValueWriter> 
  bool do_put(concurrent_btree &btr, const std::string* key_str, 
  void* val_str, ValueWriter& value_writer, bool expect_new); 


  void begin_piece();
  void abort_piece(bool unlock);
  bool commit_piece();


protected:
  void clean_up();
  bool own_insert(dbtuple* tuple);
  access_entry* read_set_find(dbtuple* tuple);

  template <typename ValueWriter> 
  std::pair< dbtuple *, bool >
  try_insert_new_tuple(concurrent_btree &btr, const std::string* key, void* value, ValueWriter& value_writer, bool occ = true);

private:
  uint64_t pid;
  uint32_t rs_start;
  uint32_t ws_start;
  Transaction* txn;
};

template <typename Transaction>
class mix_op
{

public:
  mix_op(Transaction* t);
  ~mix_op();

  void set_policy(Policy *pol);
  void set_pg(PolicyGradient *pg);

  template <typename ValueReader>
  bool do_tuple_read(dbtuple* tuple, ValueReader& value_reader, bool occ = true, uint16_t record_contention = 0);
  bool do_node_read(const typename concurrent_btree::node_opaque_t *n,
                    uint64_t v, const std::string* lkey, const std::string* ukey, bool occ = true);

  template <typename ValueReader>
  bool do_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader, uint32_t acc_id);
  template <typename ValueWriter>
  bool do_put(concurrent_btree &btr, const std::string* key_str,
              void* val_str, ValueWriter& value_writer, bool expect_new, uint32_t acc_id);

  template <typename ValueReader, typename ValueWriter>
  bool do_update(concurrent_btree &btr, const std::string *key_str,
                 ValueReader &value_reader, update_callback *callback,
                 ValueWriter &value_writer, uint32_t acc_id);

  bool do_expose_ic3_action(int pos, bool is_write, bool lock_mode = true);

  void check_rmw(int pos);

  void begin_piece();
  void abort_piece(bool unlock);
  bool commit_piece();

protected:
  template <typename ValueWriter>
  std::pair< dbtuple *, bool>
  try_insert_new_tuple(concurrent_btree &btr, const std::string* key, void* value, ValueWriter& writer, bool occ = true, concurrent_btree::node_opaque_t **node = nullptr);
  access_entry* read_set_find(dbtuple* tuple);

private:
  Transaction* txn;
  Policy *policy;
  PolicyGradient *pg;
};



template <typename Transaction>
class op_wrapper
{

private:
  enum op_type
  {
    RAW,
    ONE_ATOMIC,
    MUL_ATOMIC,
    MIX
  };

public:
  op_wrapper(Transaction* t);
  ~op_wrapper();

  template <typename ValueReader> 
  bool do_tuple_read(dbtuple* tuple, ValueReader& value_reader, bool occ = true);
  bool do_node_read(const typename concurrent_btree::node_opaque_t *n, 
                    uint64_t v, const std::string* lkey, const std::string* ukey, bool occ = true);

  template <typename ValueReader> 
  bool do_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader);
  template <typename ValueWriter> 
  bool do_put(concurrent_btree &btr, const std::string* key_str, 
              void* val_str, ValueWriter& value_writer, bool expect_new);

  template <typename ValueReader, typename ValueWriter>
  bool do_update(concurrent_btree &btr, const std::string *key_str,
                 ValueReader &value_reader, update_callback *callback,
                 ValueWriter &value_writer);

  template <typename ValueReader> 
  bool profile_do_get(concurrent_btree &btr, const std::string* key_str, ValueReader& value_reader, ic3_profile* prof);  
  

  void begin_atomic_piece(bool one_tuple);
  bool commit_atomic_piece();
  bool abort_atomic_piece();
  void abort_piece();

  inline bool is_one_atomic()
  {
    return op_s == ONE_ATOMIC;
  } 
  
private:

  raw_op<Transaction> r_op;
  atomic_mul_ops<Transaction> m_op;
  atomic_one_op<Transaction> o_op;
  mix_op<Transaction> mix_op;

  op_type op_s;
};


#endif /* _PIECE_H_ */
