#ifndef _NDB_BENCH_MICRO_H_
#define _NDB_BENCH_MICRO_H_

#include "../record/encoder.h"
#include "../record/inline_str.h"
#include "../macros.h"

#define MCSLOCK

#define WAITSYNC
//#define SINGLECTS

#define TEST_KEY_FIELDS(x, y) \
  x(int64_t,t_k_id)
#define TEST_VALUE_FIELDS(x, y) \
  x(int64_t,t_v_count) \
  y(uint64_t,t_v_tid) 

DO_STRUCT(test, TEST_KEY_FIELDS, TEST_VALUE_FIELDS)

#endif
