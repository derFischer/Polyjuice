#ifndef _SPINLOCK_XCHG_H
#define _SPINLOCK_XCHG_H

/* Spin lock using xchg.
 * Copied from http://locklessinc.com/articles/locks/
 */

/* Compile read-write barrier */
#define barrier() asm volatile("": : :"memory")

/* Pause instruction to prevent excess processor bus usage */
#define cpu_relax() asm volatile("pause\n": : :"memory")

static inline unsigned short xchg_8(volatile void *ptr, unsigned char x)
{
    __asm__ __volatile__("xchgb %0,%1"
                :"=r" (x)
                :"m" (*(volatile unsigned char *)ptr), "0" (x)
                :"memory");

    return x;
}

class spinlockxg {


private:
    char padding[64];
    volatile char lock;
    char padding1[64];

public:
    spinlockxg(): lock(0)
    {}


    void spin_lock()
    {
        while (1) {
            if (!xchg_8(&lock, 1)) return;
            while (lock) cpu_relax();
        }
    }

    inline void spin_unlock()
    {
        barrier();
        lock = 0;
    }

} __attribute__((aligned(64)));

#endif /* _SPINLOCK_XCHG_H */
