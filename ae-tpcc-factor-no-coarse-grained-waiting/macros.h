#ifndef _MACROS_H_
#define _MACROS_H_

#include <assert.h>
#include <stdexcept>

/** options */
//#define TUPLE_PREFETCH
#define BTREE_NODE_PREFETCH
//#define DIE_ON_ABORT
//#define TRAP_LARGE_ALLOOCATIONS
#define USE_BUILTIN_MEMFUNCS
//#define CHECK_INVARIANTS
//#define TUPLE_CHECK_KEY
#define USE_SMALL_CONTAINER_OPT
#define BTREE_NODE_ALLOC_CACHE_ALIGNED
#define TXN_BTREE_DUMP_PURGE_STATS
//#define ENABLE_EVENT_COUNTERS
//#define ENABLE_BENCH_TXN_COUNTERS
#define USE_VARINT_ENCODING
//#define DISABLE_FIELD_SELECTION
//#define PARANOID_CHECKING
//#define BTREE_LOCK_OWNERSHIP_CHECKING
//#define TUPLE_LOCK_OWNERSHIP_CHECKING
//#define MEMCHECK_MAGIC 0xFF
//#define TUPLE_MAGIC
//#define PROTO2_CAN_DISABLE_GC
#define PROTO2_CAN_DISABLE_SNAPSHOTS
//#define USE_PERF_CTRS


// XXX(Conrad): Turn of pipeline commit cuz deadlock
//#define PIPELINE_COMMIT
#define USE_MCS_LOCK
#define ENABLE_RENDEZVOUS
#define USE_ONE_OP_PIECE

// XXX(Conrad): Turn off cummatative_set
//#define SUPPORT_COMMUTATIVE_OP
// #define TRACK_LAST_ENTRY
 #define CASCADING_ABORT_BY_EXCEPTION

#ifndef CONFIG_H
#error "no CONFIG_H set"
#endif

#include CONFIG_H

/**
 * some non-sensical options, which only make sense for performance debugging
 * experiments. these should ALL be DISABLED when doing actual benchmarking
 **/
//#define LOGGER_UNSAFE_FAKE_COMPRESSION
//#define LOGGER_UNSAFE_REDUCE_BUFFER_SIZE
//#define LOGGER_STRIDE_OVER_BUFFER

#define CACHELINE_SIZE 64 // XXX: don't assume x86
#define LG_CACHELINE_SIZE __builtin_ctz(CACHELINE_SIZE)

// global maximum on the number of unique threads allowed
// in the system
#define NMAXCOREBITS 9
#define NMAXCORES    (1 << NMAXCOREBITS)

// some helpers for cacheline alignment
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))

#define __XCONCAT2(a, b) a ## b
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define CACHE_PADOUT  \
    char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHELINE_SIZE)))
#define PACKED __attribute__((packed))

#define NEVER_INLINE  __attribute__((noinline))
#define ALWAYS_INLINE __attribute__((always_inline))
#define UNUSED __attribute__((unused))

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")

#ifdef NDEBUG
  #define ALWAYS_ASSERT(expr) (likely((expr)) ? (void)0 : abort())
#else
  #define ALWAYS_ASSERT(expr) assert((expr))
#endif /* NDEBUG */

#define ARRAY_NELEMS(a) (sizeof(a)/sizeof((a)[0]))

#define VERBOSE(expr) ((void)0)
//#define VERBOSE(expr) (expr)

#ifdef CHECK_INVARIANTS
  #define INVARIANT(expr) ALWAYS_ASSERT(expr)
#else
  #define INVARIANT(expr) ((void)0)
#endif /* CHECK_INVARIANTS */

// XXX: would be nice if we checked these during single threaded execution
#define SINGLE_THREADED_INVARIANT(expr) ((void)0)

// tune away
#define SMALL_SIZE_VEC       128
#define SMALL_SIZE_MAP       128
#define EXTRA_SMALL_SIZE_MAP 8

//#define BACKOFF_SPINS_FACTOR 1000
//#define BACKOFF_SPINS_FACTOR 100
#define BACKOFF_SPINS_FACTOR 10

// throw exception after the assert(), so that GCC knows
// we'll never return
#define NDB_UNIMPLEMENTED(what) \
  do { \
    ALWAYS_ASSERT(false); \
    throw ::std::runtime_error(what); \
  } while (0)

#ifdef USE_BUILTIN_MEMFUNCS
#define NDB_MEMCPY __builtin_memcpy
#define NDB_MEMSET __builtin_memset
#else
#define NDB_MEMCPY memcpy
#define NDB_MEMSET memset
#endif

#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 7)
#define GCC_AT_LEAST_47 1
#else
#define GCC_AT_LEAST_47 0
#endif

// g++-4.6 does not support override
#if GCC_AT_LEAST_47
#define OVERRIDE override
#else
#define OVERRIDE
#endif

// number of nanoseconds in 1 second (1e9)
#define ONE_SECOND_NS 1000000000

// microseconds, 0 means infinite waiting
// this could be set longer, since ideally we want the agent to learn a policy not relying on timeout 
#define TIME_OUT 100000

// batch read modify write as a update operation
//#define USE_UPDATE_FUNC

// in waitAccessInfo, WAIT_DEP_COMMIT means waiting for the specific txn to commit
// Different w/ waitCommit: waitCommit means waiting for all txns in dependency queue to commit
#define WAIT_DEP_COMMIT 99

// indicating how much independent txn can a bench worker can hold at most
#define MAX_TXN_BUF_SIZE 32

#endif /* _MACROS_H_ */
