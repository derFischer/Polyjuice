#ifndef _MCS_SPINLOCK
#define _MCS_SPINLOCK

#include "amd64.h"
#include <pthread.h>

class mcslock {

public:
    struct qnode_t
    {
        volatile qnode_t * volatile next;
        volatile int spin;

        constexpr qnode_t(): next(NULL), spin(0){}
    };


private:
    struct lock_t
    {
        qnode_t* tail;
        lock_t(): tail(NULL){}
    }PACKED;

    volatile lock_t lock_;
public:

    // return whether there is other one acquiring mcslock
    bool acquire(qnode_t *me)
    {
        qnode_t *tail = NULL;
        
        me->next = NULL;
        me->spin = 0;

        tail = __sync_lock_test_and_set(&lock_.tail, me);
            
        memory_barrier();
        /* No one there? */
        if (!tail){ 
            return false; 
        }

        memory_barrier();

        /* Someone there, need to link in */
        tail->next = me;

        /* Make sure we do the above setting of next. */
        memory_barrier();

        /* Spin on my spin variable */
        while (!me->spin){
	      memory_barrier();
          nop_pause();
	}

        return true;
    }

    // return whether there is other one acquiring mcslock
    bool release(qnode_t* me)
    {
        /* No successor yet? */
        if (!me->next)
        {
            /* Try to atomically unlock */
            if (__sync_bool_compare_and_swap(&lock_.tail, me, NULL)) return false;
            
            memory_barrier();
            /* Wait for successor to appear */
            while (!me->next){
              nop_pause();
              memory_barrier();
            } 


        }

            
        memory_barrier();

        /* Unlock next one */
        me->next->spin = 1;     

        return true; 
    }

} PACKED; 

template <typename MCSLockable>
class mcs_lock_guard {

public:
  mcs_lock_guard(MCSLockable *l, mcslock::qnode_t* args)
    : l(l), node(args)
  {
    if (likely(l)) {
      l->mcs_lock(node);
    }
  }

  ~mcs_lock_guard()
  {
    if (likely(l))
      l->mcs_unlock(node);
  }

private:
    MCSLockable* l;
    mcslock::qnode_t *node;
};

#endif
