#ifndef _TXN_ENTRY_IMPL_H_
#define _TXN_ENTRY_IMPL_H_
#include <stdint.h>
#include <string>
#include "tuple.h"



template <typename Reader, typename StringAllocator>
bool 
access_entry::read_record(Reader &reader, StringAllocator &sa)
{

  if(check_flag(status::DELETED)) {
    //The record is deleted
    return false;
  } else if (value != nullptr) {
    //Return the update value
    char* v = const_cast<char *>(value->data());
    return reader((uint8_t *)v, value->size(), sa); 
  }


  if(prev != nullptr)
    return prev->read_record(reader, sa);
  else {
    INVARIANT(target != nullptr);
    return reader(((dbtuple *)target)->get_value_start(), ((dbtuple *)target) ->size, sa);
  }

  return false;
}



template <typename StringAllocator>
void
access_entry::write(void* val, uint64_t commit_pid, StringAllocator& sa)
{
  
  INVARIANT(commit_pid == txn.pid);

  set_flag(WRITTEN);
  
  if(!val) {
    set_delete();
    value = nullptr;
    return;
  }

  if(value == nullptr) {
    value = sa();
  }
  
  std::string* str = reinterpret_cast<std::string *>(val);
  value->assign(*str);

  if(is_delete())
    clear_delete();
}

#endif