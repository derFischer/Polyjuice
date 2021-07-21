#ifndef _NDB_BENCH_MICRO_PIECES_H_
#define _NDB_BENCH_MICRO_PIECES_H_

#include "../record/encoder.h"
#include "../record/inline_str.h"
#include "../macros.h"

#define TEST_KEY_FIELDS(x, y) \
  x(int64_t,t_k)
#define TEST_VALUE_FIELDS(x, y) \
  x(int64_t,t_v_count)
DO_STRUCT(piece_test, TEST_KEY_FIELDS, TEST_VALUE_FIELDS)

#endif
