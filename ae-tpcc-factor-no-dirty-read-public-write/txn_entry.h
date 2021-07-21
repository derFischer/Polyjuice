#ifndef _TXN_ENTRY_H_
#define _TXN_ENTRY_H_
#include <stdint.h>
#include <string>
#include "macros.h"
#include "mcs_spinlock.h"


//Performance Profiling Purpose

struct ic3_profile {

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

#define MAX_PROFILE_LEVEL 15

  uint64_t data[MAX_PROFILE_LEVEL];
  uint32_t count; 
  uint32_t padding; 

  ic3_profile()
  {
    for(int i = 0; i < MAX_PROFILE_LEVEL; i++)
      data[i] = 0;
    count = 0;
  }

  
} CACHE_ALIGNED;

// struct dbtuple;

struct txn_entry {

  uint64_t pid;
  uint64_t tid;
  void* txn;

  constexpr txn_entry(uint64_t pid, uint64_t tid, void *txn)
    :pid(pid)
     ,tid(tid)
     ,txn(txn){}

};

struct txn_wait_entry
{
  bool dirty_read_dep;
  uint64_t tid;
  void* txn;
  
  constexpr txn_wait_entry(bool dirty_read_dep, uint64_t tid, void *txn)
    :dirty_read_dep(dirty_read_dep)
     ,tid(tid)
     ,txn(txn){} 
};


class access_entry {

  friend struct dbtuple;
  template <typename Transaction> friend class mix_op;
  
protected:

  enum status
  {
    RECIEVED = 0,
    WRITTEN,
    INSERTED,
    DELETED,
    LINKED,
    OVERWRITE,
  };

  enum action
  {
    PENDING = 0,
    ABORT,
    COMMIT,
  };

public:
  volatile uint8_t act;

  //target tuple to commit
  void *target;

  //value read or written, nullptr if delete
  std::string* value;

  //For using mcslock
  mcslock::qnode_t m_node;

  txn_entry txn;

  volatile uint8_t sflag;

  //MAXCPU is 256
  volatile uint16_t readers;

protected:
  access_entry* volatile prev;
  access_entry* volatile next;

  // write operation id for a txn
  int write_seq; 

public:
  constexpr access_entry(uint64_t pid, uint64_t tid, void *txn): 
    act(action::PENDING)
    , target(nullptr)
    , value(nullptr)
    , txn(pid, tid, txn)
    , sflag(0)
    , readers(0)
    , prev(nullptr)
    , next(nullptr)
    , write_seq(-1) {}

  ~access_entry(){}

  const std::string* get_value() const;
  
  void set_pid(uint64_t p);
  uint64_t get_pid();
  uint64_t get_tid();
  void* get_txn();

  bool is_write();
  bool is_overwrite();
  void set_overwrite();

  bool is_insert();
  void set_insert();
  void clear_insert();
  
  bool is_linked();
  void set_linked();
  void clear_linked();
  
  void set_delete();
  bool is_delete();
  void clear_delete();

  void* get_commit_target();

  void reset_target(void* target_tuple);
  void set_prepare(void* target_tuple);
  bool is_prepare();
  void set_doomed();
  bool is_doomed();

  uint16_t inc_reader();
  uint16_t dec_reader();
  bool has_reader();

  access_entry* get_next();
  access_entry* get_prev();

  int get_write_seq();
  void set_write_seq(int seq);

  template <typename Reader, typename StringAllocator>
  bool 
  read_record(Reader &reader, StringAllocator &sa);
  

  // template <typename StringAllocator>
  // void 
  // recieve_val(uint8_t* val, size_t size, StringAllocator& sa)
  // {
  //   if(size == 0) {
  //     value = nullptr;
  //   }
  //   else {
  //     value = sa();
  //     value->assign((const char *)val, size);
  //   }
  // }

  // template <typename StringAllocator>
  // void 
  // recieve_val(access_entry* last_acc, StringAllocator& sa)
  // {
  //   if(last_acc->is_delete() || !last_acc->get_value()) {
  //     value = nullptr;
  //   } else {
  //     value = sa();
  //     value->assign(last_acc->get_value()->data(), last_acc->get_value()->length());
  //   }
  // }

  template <typename StringAllocator>
  void
  write(void* val, uint64_t commit_pid, StringAllocator& sa);
  
protected:

  bool check_flag(status shift);
  void set_flag(status shift);
  void clear_flag(status shift);

}CACHE_ALIGNED;




#endif 
