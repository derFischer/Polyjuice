#ifndef _RWLOCK_H_
#define _RWLOCK_H_

#include <stdint.h>
#include <cstdint>

#include "amd64.h"
#include "macros.h"
#include "util.h"
#include "pthread.h" 

#include <atomic>

/************************************************/
// LIST helper (read from head & write to tail)
/************************************************/
#define LIST_GET_HEAD(lhead, ltail, en) {\
	en = lhead; \
	lhead = lhead->next; \
	if (lhead) lhead->prev = NULL; \
	else ltail = NULL; \
	en->next = NULL; }
#define LIST_PUT_TAIL(lhead, ltail, en) {\
	en->next = NULL; \
	en->prev = NULL; \
	if (ltail) { en->prev = ltail; ltail->next = en; ltail = en; } \
	else { lhead = en; ltail = en; }}
#define LIST_INSERT_BEFORE(entry, newentry) { \
	newentry->next = entry; \
	newentry->prev = entry->prev; \
	if (entry->prev) entry->prev->next = newentry; \
	entry->prev = newentry; }
#define LIST_REMOVE(entry) { \
	if (entry->next) entry->next->prev = entry->prev; \
	if (entry->prev) entry->prev->next = entry->next; }
#define LIST_REMOVE_HT(entry, head, tail) { \
	if (entry->next) entry->next->prev = entry->prev; \
	else { assert(entry == tail); tail = entry->prev; } \
	if (entry->prev) entry->prev->next = entry->next; \
	else { assert(entry == head); head = entry->next; } \

/************************************************/
// STACK helper (push & pop)
/************************************************/
#define STACK_POP(stack, top) { \
	if (stack == NULL) top = NULL; \
	else {	top = stack; 	stack=stack->next; } }
#define STACK_PUSH(stack, entry) {\
	entry->next = stack; stack = entry; }\

#define LOCK_EX 0
#define LOCK_SH 1
#define LOCK_NONE 2

struct LockEntry {
    uint16_t type;
    uint64_t tid;
    volatile bool lock_ready;
	LockEntry * next;
	LockEntry * prev;
};

class RWLock {
public:
	RWLock()
    {
        owners = NULL;
        waiters_head = NULL;
        waiters_tail = NULL;
        owner_cnt = 0;

        latch = new pthread_mutex_t;
        pthread_mutex_init(latch, NULL);
        
        lock_type = LOCK_NONE;
    }

    bool lockW(uint64_t tid, bool not_sorted = true)
    {
        return lock_get(LOCK_EX, tid, not_sorted);
    }

    void unlockW(uint64_t tid)
    {
        return lock_release(tid);
    }

    bool lockR(uint64_t tid, bool not_sorted = true)
    {
        return lock_get(LOCK_SH, tid, not_sorted);
    }

    void unlockR(uint64_t tid)
    {
        return lock_release(tid);
    }

	// [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
    bool lock_get(uint16_t type, uint64_t tid, bool not_sorted)
    {
        pthread_mutex_lock(latch);
        bool conflict = conflict_lock(lock_type, type);
        if (!conflict) 
        {
            //if the lock doesn't conflict and it is older than the waiter, it has to add itself to the list (otherwise the waiter should be aborted)
            if (waiters_head && tid < waiters_head->tid)
                conflict = true;
        }
        
        if (conflict) 
        { 
            ///////////////////////////////////////////////////////////
            //  - T is the txn currently running
            //	IF T.ts > ts of all owners
            //		T can wait
            //  ELSE
            //      T should abort
            //////////////////////////////////////////////////////////

            bool canwait = true;

            //if the benchmark is not sorted, check the timestamp. otherwise, it can directly wait
            LockEntry * en = owners;
            if (not_sorted)
            {
                while (en != NULL) 
                {
                    if (en->tid < tid) 
                    {
                        canwait = false;
                        break;
                    }
                    en = en->next;
                }
            }
            if (canwait) 
            {
                // insert txn to the right position
                // the waiter list is always in timestamp order
                LockEntry * entry = get_entry();
                entry->tid = tid;
                entry->type = type;
                entry->lock_ready = false;
                en = waiters_head;
                while (en != NULL && tid < en->tid) 
                    en = en->next;
                if (en) 
                {
                    LIST_INSERT_BEFORE(en, entry);
                    if (en == waiters_head)
                        waiters_head = entry;
                } else 
                    LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
                
                //release mutex
                pthread_mutex_unlock(latch);
                while (!entry->lock_ready)
                {
                    nop_pause();
                }
                return true;
            }
            else
            {
                pthread_mutex_unlock(latch);
                return false;
            }
        } 
        else 
        {
            LockEntry * entry = get_entry();
            entry->type = type;
            entry->tid = tid;
            STACK_PUSH(owners, entry);
            owner_cnt ++;
            lock_type = type;
            pthread_mutex_unlock(latch);
            return true;
        }
    }

    void lock_release(uint64_t tid)
    {
        pthread_mutex_lock(latch);
        // Try to find the entry in the owners
        LockEntry * en = owners;
        LockEntry * prev = NULL;

        while (en != NULL && en->tid != tid) {
            prev = en;
            en = en->next;
        }
        if (en) { // find the entry in the owner list
            if (prev) prev->next = en->next;
            else owners = en->next;
            return_entry(en);
            owner_cnt --;
            if (owner_cnt == 0)
                lock_type = LOCK_NONE;
        } else {
            // Not in owners list, try waiters list.
            en = waiters_head;
            while (en != NULL && en->tid != tid)
                en = en->next;
            LIST_REMOVE(en);
            if (en == waiters_head)
                waiters_head = en->next;
            if (en == waiters_tail)
                waiters_tail = en->prev;
            return_entry(en);
        }

        LockEntry * entry;
        // If any waiter can join the owners, just do it!
        while (waiters_head && !conflict_lock(lock_type, waiters_head->type)) {
            LIST_GET_HEAD(waiters_head, waiters_tail, entry);
            STACK_PUSH(owners, entry);
            owner_cnt ++;
            entry->lock_ready = true;
            lock_type = entry->type;
        }
        pthread_mutex_unlock(latch);
    }

private:
    pthread_mutex_t * latch;
    uint16_t lock_type;
    uint32_t owner_cnt;

	// owners is a single linked list
	// waiters is a double linked list
	// [waiters] head is the oldest txn, tail is the youngest txn.
	//   So new txns are inserted into the tail.
	LockEntry * owners = NULL;
	LockEntry * waiters_head = NULL;
	LockEntry * waiters_tail = NULL;

	bool conflict_lock(uint16_t l1, uint16_t l2)
    {
        if (l1 == LOCK_NONE || l2 == LOCK_NONE)
            return false;
        else if (l1 == LOCK_EX || l2 == LOCK_EX)
            return true;
        else
            return false;
    }
	LockEntry * get_entry() {
        return new LockEntry();
    }
	void return_entry(LockEntry * entry) {
        delete entry;
    }
};

#endif