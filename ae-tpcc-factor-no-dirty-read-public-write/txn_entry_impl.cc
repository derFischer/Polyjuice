#ifndef _TXN_ENTRY_IMPL_H_
#define _TXN_ENTRY_IMPL_H_

#include "txn_entry.h"
#include "macros.h"
#include "core.h"

void* access_entry::get_txn()
{
  return txn.txn;
}


uint64_t access_entry::get_tid()
{
  return txn.tid;
}

uint64_t access_entry::get_pid()
{
  return txn.pid;
}

void access_entry::set_pid(uint64_t p)
{
  txn.pid = p;
}

uint16_t access_entry::inc_reader()
{
  uint16_t cur = __sync_add_and_fetch(&readers, 1);
  INVARIANT(cur < 64);
  return cur;
}

uint16_t access_entry::dec_reader()
{
  uint16_t cur = __sync_sub_and_fetch(&readers, 1);
  INVARIANT(cur < 64);
  return cur;
}

bool access_entry::has_reader()
{
  return readers > 0;
}

const std::string* access_entry::get_value() const
{
  return value;
}

bool access_entry::is_write()
{
  return check_flag(status::WRITTEN);
}


bool access_entry::is_overwrite()
{
  return check_flag(status::OVERWRITE);
}

void access_entry::set_overwrite()
{
  set_flag(status::OVERWRITE);
}


bool access_entry::is_insert()
{
 return check_flag(status::INSERTED); 
}

void access_entry::set_insert()
{
  set_flag(status::INSERTED);
}

void access_entry::clear_insert()
{
  clear_flag(status::INSERTED);
}

void access_entry::set_delete()
{
  set_flag(status::DELETED);
}


bool access_entry::is_delete()
{
  return check_flag(status::DELETED);
}

void access_entry::clear_delete()
{
  clear_flag(status::DELETED);
}


bool access_entry::is_linked()
{
  return check_flag(status::LINKED);
}


void access_entry::set_linked()
{
  set_flag(status::LINKED);
}

void access_entry::clear_linked()
{
  clear_flag(status::LINKED);
}


bool access_entry::check_flag(status shift)
{
  return ((sflag & (1 << shift)) != 0 );
}


void access_entry::set_flag(status shift)
{
  sflag |= (1 << shift);
}

void access_entry::clear_flag(status shift)
{
  sflag &= ~(1 << shift);
}

void* access_entry::get_commit_target()
{
  INVARIANT(target);
  return target;
}

void access_entry::reset_target(void* target_tuple)
{
  //fprintf(stderr, "e: [%lx] reset target %lx\n", this, target_tuple);
  INVARIANT(target_tuple);
  INVARIANT(target && target != target_tuple);

  target = target_tuple;
}

access_entry* access_entry::get_next()
{
  return next;
}

access_entry* access_entry::get_prev()
{
  return prev;
}


void access_entry::set_prepare(void* target_tuple)
{
  //fprintf(stderr, "e: [%lx] set target %lx\n", this, target_tuple);
  INVARIANT(!target && target_tuple);
  target = target_tuple;
  act = action::COMMIT;
}

bool access_entry::is_prepare()
{
  return act == action::COMMIT;
}

void access_entry::set_doomed()
{
  act = action::ABORT;
}

bool access_entry::is_doomed()
{
  return act == action::ABORT;
}

int access_entry::get_write_seq()
{
  return write_seq;
}

void access_entry::set_write_seq(int seq)
{
  write_seq = seq;
}
#endif