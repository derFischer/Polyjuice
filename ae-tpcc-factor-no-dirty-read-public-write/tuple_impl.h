#ifndef _TUPLE_IMPL_H_
#define _TUPLE_IMPL_H_
#include <stdint.h>
#include <string>
#include "tuple.h"
#include "txn_entry_impl.h"



template <typename Reader, typename StringAllocator>
inline ALWAYS_INLINE bool
dbtuple::raw_stable_read(Reader &reader, StringAllocator &sa, bool occ,
                         tid_t* tid)
{

    // if(sub_version == MAX_TID) {
    //   return false;
    // }

retry:

    // tid_t sv = sub_version;

    bool res = false;

    memory_barrier();
    if(nullptr == access_tail || occ) {
      
      // if(sub_version == MAX_TID) {
      //   return false;
      // }

      const version_t v = reader_stable_version(false);

      if(IsDeleting(v)) {
        res = false;
      } else {
        if (tid)
          *tid = this->version;
        res = reader(get_value_start(), size, sa);
      }

      if(unlikely(!reader_check_version(v)))
        goto retry;

    } else {


      lock_tail();

      access_entry *tail = access_tail;

      if(unlikely(!tail)) {
        unlock_tail();
        goto retry;
      }

      tail->inc_reader();

      unlock_tail();

      if(tail->is_delete() ||
        (!tail->is_write() && !tail->get_value())){
        res = false;
      } else {
        res = tail->read_record(reader, sa);
      }

      tail->dec_reader();

    }

    //Double check to reduce the abort rate
    // if(sub_version != sv)
    //   goto retry;

   return res;

}

template <typename Reader, typename StringAllocator>
inline ALWAYS_INLINE bool
dbtuple::ic3_stable_read(Reader &reader, tid_t& start_p, StringAllocator &sa, access_entry* e)
{

#if 1
    // not used in chamcc
    ALWAYS_ASSERT(false);
#else
    if(sub_version == MAX_TID) {
      start_p = MIN_TID;
      return false;
    }

    

retry:

    bool res = false;
    tid_t sv = sub_version;

    // bool is_delete = false;

    memory_barrier();
    if(nullptr == access_tail) {
      
      if(sub_version == MAX_TID) {
        start_p = MIN_TID;
        return false;
      }

      const version_t v = reader_stable_version(false);

      if(IsDeleting(v)) {
        // is_delete = true;
        res = false;
      } else {
        res = reader(get_value_start(), size, sa);
        // e->recieve_val(get_value_start(), size, sa);  
      }

      memory_barrier();

      if(unlikely(!reader_check_version(v)))
        goto retry;

    } else {

      lock_tail();

      access_entry* tail = access_tail;

      if(unlikely(!tail)) {
        unlock_tail();
        goto retry;
      }

      tail->inc_reader();

      unlock_tail();


      if(tail->is_write() && tail->is_delete()){
        res = false;
      } else {
        res = tail->read_record(reader, sa);
        // e->recieve_val(tail, sa);  
      }

      memory_barrier();
      tail->dec_reader();

    }

    //Double check to reduce the abort rate
    memory_barrier();
    if(sub_version != sv)
      goto retry;

    start_p = sv;

    return res;

    // if(is_delete)
    //   return false;
    
    // return e->read_record(reader, sa);
#endif
}


//Called when a lock is hold
template <typename Reader, typename StringAllocator>
inline ALWAYS_INLINE bool
dbtuple::ic3_read(Reader &reader, StringAllocator &sa, access_entry* e)
{

    INVARIANT(is_locked());

//    if(sub_version == MAX_TID) {
//      return false;
//    }

    memory_barrier();
    if(nullptr == access_tail) {

//      if(sub_version == MAX_TID || is_deleting()) {
      if (version == MAX_TID || is_deleting()) {
        return false;
      }

      return reader(get_value_start(), size, sa);  

      // e->recieve_val(get_value_start(), size, sa);  

    } else {

      access_entry* tail = access_tail;

      if(tail->is_write() && tail->is_delete()){
        return false;
      } 

      return tail->read_record(reader, sa);

      // e->recieve_val(tail, sa);  
    }
    
    // return e->read_record(reader, sa);

    return false;
}


template <typename Reader, typename StringAllocator>
inline ALWAYS_INLINE bool
dbtuple::profile_ic3_read(Reader &reader, StringAllocator &sa, access_entry* e, ic3_profile* prof)
{
#if 1
    // not used in chamcc
    ALWAYS_ASSERT(false);
#else
    INVARIANT(is_locked());

    if(sub_version == MAX_TID) {
      return false;
    }

    memory_barrier();
    if(nullptr == access_tail) {
      
      if(sub_version == MAX_TID 
        || is_deleting()) {
        return false;
      }

      uint64_t beg = rdtsc();
      bool res = reader(get_value_start(), size, sa);  
      prof->data[ic3_profile::level::LEVEL5] += rdtsc() - beg;

      return res;
      // e->recieve_val(get_value_start(), size, sa);  

    } else {

      access_entry* tail = access_tail;

      if(tail->is_write() && tail->is_delete()){
        return false;
      } 

      return tail->read_record(reader, sa);

      // e->recieve_val(tail, sa);  
    }
    
    // return e->read_record(reader, sa);

    return false;
#endif
}

#endif
