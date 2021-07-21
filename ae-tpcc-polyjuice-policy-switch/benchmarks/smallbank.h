#ifndef _NDB_BENCH_SEATS_H_
#define _NDB_BENCH_SEATS_H_

#include "../record/encoder.h"
#include "../record/inline_str.h"
#include "../macros.h"
#include "../tuple.h"
#include "bench.h"



#define ACCOUNT_KEY_FIELDS(x,y) \
	x(int32_t, cust_partition) \
	y(int64_t, cust_id)
#define ACCOUNT_VALUE_FIELDS(x, y) \
	x(inline_str_16<64>, cust_name) 
DO_STRUCT(account,ACCOUNT_KEY_FIELDS,ACCOUNT_VALUE_FIELDS);


#define ACCOUNT_IDX_KEY_FIELDS(x,y) \
	x(int32_t, cust_partition) \
	y(inline_str_16<64>, cust_name)
#define ACCOUNT_IDX_VALUE_FIELDS(x, y) \
	x(int64_t, cust_id)
DO_STRUCT(account_index,ACCOUNT_IDX_KEY_FIELDS,ACCOUNT_IDX_VALUE_FIELDS);

#define CHECKING_KEY_FIELDS(x,y) \
	x(int32_t, cust_partition) \
	y(int64_t, cust_id)
#define CHECKING_VALUE_FIELDS(x, y) \
	x(float, balance) 
DO_STRUCT(checking,CHECKING_KEY_FIELDS,CHECKING_VALUE_FIELDS);


#define SAVING_KEY_FIELDS(x,y) \
	x(int32_t, cust_partition) \
	y(int64_t, cust_id)
#define SAVING_VALUE_FIELDS(x, y) \
	x(float, balance) 
DO_STRUCT(saving,SAVING_KEY_FIELDS,SAVING_VALUE_FIELDS);










#endif
