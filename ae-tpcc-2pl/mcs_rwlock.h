#ifndef _MCS_RWLOCK_H_
#define _MCS_RWLOCK_H_
// refs: https://www.cs.rochester.edu/research/synchronization/pseudocode/rw.html
#include "macros.h"

#define fqrwl_cpu_relax() asm volatile( "pause\n" : : : "memory" )
#define fqrwl_barrier() asm volatile( "mfence" : : : "memory" )

#define FQRWL_CLASS_READER          0x1
#define FQRWL_CLASS_WRITER          0x2
#define FQRWL_CLASS_NONE            0x0

// atomic operations required

// fetch_and_store
#define fqrwl_fetch_and_store(P, V) \
    __sync_lock_test_and_set((P), (V))

// compare_and_store
#define fqrwl_compare_and_store(P, O, N) \
    __sync_bool_compare_and_swap((P), (O), (N))

// atomic_increment
#define fqrwl_atomic_increment(P) \
    __sync_fetch_and_add((P), 1)

// fetch_and_decrement
#define fqrwl_fetch_and_decrement(P) \
    __sync_fetch_and_add((P), -1)


#define USE_MCS_SPINLOCK 1
#define USE_SIMPLE_LOCK 0

extern std::atomic<uint64_t> n_contention;

class mcs_rwlock {
public:
    struct state_t {
        volatile uint16_t blocked_;
        volatile uint16_t successor_class_;
    };
    struct qnode_t {
        volatile uint8_t class_;
        volatile qnode_t *next_;
        state_t state_; //XXX pading
        qnode_t(): class_(0), next_(NULL){}
    };

private:
    typedef uint32_t state_val_t; // dummy type, corresponding to sizeof(state_t)
    struct lock_t {
        union {
            volatile qnode_t *tail_;
            // char padding1[64];
        };
        union {
            volatile uint32_t reader_count_;
            // char padding2[64];
        };
        union {
            volatile qnode_t *next_writer_;
            // char padding3[64];
        };

        lock_t() : tail_(NULL), reader_count_(0), next_writer_(NULL) {}
    } PACKED;

    lock_t lock_;

    static void start_write(lock_t *lock, qnode_t *me)
    {
        me->class_ = FQRWL_CLASS_WRITER;
        me->next_ = NULL;
        me->state_ = (state_t) { 1/*blocked*/, FQRWL_CLASS_NONE };
        volatile qnode_t *pred = fqrwl_fetch_and_store(&lock->tail_, (volatile qnode_t *)me);
        if (pred == NULL) {

            lock->next_writer_ = me;
            fqrwl_barrier();

            if (lock->reader_count_ == 0 &&
                fqrwl_fetch_and_store(&lock->next_writer_, NULL) == me) {
                // no reader who will resume me
                me->state_.blocked_ = 0;

                // ALGORITHM: do nothing
                // CHANGE: directly return
                return;
            }
        } else {
            // must update successor_class before updating next
            pred->state_.successor_class_ = FQRWL_CLASS_WRITER;
            fqrwl_barrier();
            pred->next_ = me;
        }

        // n_contention++;
        while(me->state_.blocked_) fqrwl_cpu_relax();
    }

    static void end_write(lock_t *lock, qnode_t *me)
    {
        if (me->next_ != NULL ||
            !fqrwl_compare_and_store(&lock->tail_, me, NULL)) {
            // wait until succ inspects my state
            while (me->next_ == NULL)
                fqrwl_cpu_relax();

            fqrwl_barrier();
            if (me->next_->class_ == FQRWL_CLASS_READER)
                fqrwl_atomic_increment(&lock->reader_count_);

            // FIXME do we really need memory barrier here?
            fqrwl_barrier();
            me->next_->state_.blocked_ = 0;
        }
    }

    static void start_read(lock_t *lock, qnode_t *me)
    {
        me->class_ = FQRWL_CLASS_READER;
        me->next_ = NULL;
        me->state_ = (state_t) { 1/*blocked*/, FQRWL_CLASS_NONE };

        volatile qnode_t *pred = fqrwl_fetch_and_store(&lock->tail_, (volatile qnode_t *)me);
        if (pred == NULL) {
            fqrwl_atomic_increment(&lock->reader_count_);
            me->state_.blocked_ = 0;
        } else {
            state_t oldval = (state_t) { 1, FQRWL_CLASS_NONE };
            state_t newval = (state_t) { 1, FQRWL_CLASS_READER };
            state_val_t *oldval_ptr = (state_val_t *)&oldval;
            state_val_t *newval_ptr = (state_val_t *)&newval;
            if (pred->class_ == FQRWL_CLASS_WRITER ||
                fqrwl_compare_and_store(
                    (state_val_t *)&pred->state_,
                    *oldval_ptr,
                    *newval_ptr)) {
                // pred is a writer, or a waiting reader
                // pred will increment reader_count and release me
                pred->next_ = me;

                // ALGORITHM: while(me->state_.blocked_); // spin on blocked
                // CHANGE: yield, spin after resuemed
                fqrwl_barrier();

                while(me->state_.blocked_) fqrwl_cpu_relax();
            } else {
                // increment reader_count and go
                fqrwl_atomic_increment(&lock->reader_count_);
                pred->next_ = me;
                me->state_.blocked_ = 0;
            }
        }

        fqrwl_barrier();
        // cascade release read locks
        if (me->state_.successor_class_ == FQRWL_CLASS_READER) {
            while (me->next_ == NULL)
                fqrwl_cpu_relax();
            fqrwl_atomic_increment(&lock->reader_count_);

            // ALGORITHM: me->next_->state_.blocked_ = 0; // cascade release read locks
            // CHANGE: push to runnable queue, resume it.
            // FIXME do we really need memory barrier here?
            fqrwl_barrier();
            me->next_->state_.blocked_ = 0;
        }
    }

    static void end_read(lock_t *lock, qnode_t *me)
    {
        if (me->next_ != NULL ||
            !fqrwl_compare_and_store(&lock->tail_, me, NULL)) {
            // wait until successor inspects my state
            while (me->next_ == NULL)
                fqrwl_cpu_relax();
            fqrwl_barrier();
            if (me->state_.successor_class_ == FQRWL_CLASS_WRITER)
                lock->next_writer_ = me->next_;
        }

        volatile qnode_t *w;
        if (fqrwl_fetch_and_decrement(&lock->reader_count_) == 1 &&
            (w = lock->next_writer_) != NULL &&
            lock->reader_count_ == 0 &&
            fqrwl_compare_and_store(&lock->next_writer_, w, NULL)) {

            // ALGORITHM: w->state_.blocked_ = 0; // release lock for w
            // CHANGE: push to runnable queue, resume it.
            // FIXME do we really need memory barrier here?
            fqrwl_barrier();
            w->state_.blocked_ = 0;
        }
    }

public:
    
    void start_write(qnode_t *qnode)
    {
        start_write(&lock_, qnode);
    }


    void end_write(qnode_t* qnode)
    {
        end_write(&lock_, qnode);   
    }

    void start_read(qnode_t* qnode)
    {
        start_read(&lock_, qnode);
    }

    void end_read(qnode_t* qnode)
    {
        end_read(&lock_, qnode);
    }


} PACKED;


#endif
